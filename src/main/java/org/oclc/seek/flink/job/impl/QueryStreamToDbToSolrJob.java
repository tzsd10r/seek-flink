/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.com.google.common.collect.ImmutableMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.oclc.seek.flink.document.KbwcEntryDocument;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.mapper.DBFetcherCallBack;
import org.oclc.seek.flink.mapper.DocumentParser;
import org.oclc.seek.flink.mapper.JsonTextParser;
import org.oclc.seek.flink.record.EntryFind;
import org.oclc.seek.flink.sink.SolrSink;
import org.oclc.seek.flink.source.QueryLikeSource;

/**
 * Here, you can start creating your execution plan for Flink.
 * <p>
 * Start with getting some data from the environment, as follows:
 *
 * <pre>
 * env.readTextFile(textPath);
 * </pre>
 *
 * ...then, transform the resulting DataStream<T> using operations like the following:
 * <p>
 * .filter() <br>
 * .flatMap() <br>
 * .join() <br>
 * .group()
 * <p>
 * ...and many more.
 * <p>
 * Have a look at the programming guide and examples:
 * <p>
 * http://flink.apache.org/docs/latest/programming_guide.html<br>
 * http://flink.apache.org/docs/latest/examples.html <br>
 */
public class QueryStreamToDbToSolrJob extends JobGeneric {
    private static final long serialVersionUID = 1L;

    @Override
    public void init() {
        super.init();
    }

    /**
     * @throws Exception
     */
    @Override
    public void execute(final StreamExecutionEnvironment env) throws Exception {
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        String zkHosts = parameterTool.getRequired(SolrSink.ZKHOSTS);
        String collection = parameterTool.getRequired(SolrSink.COLLECTION);
        Map<String, String> configMap = ImmutableMap.of(SolrSink.ZKHOSTS, zkHosts, SolrSink.COLLECTION, collection);

        DataStream<String> queries = env.addSource(new QueryLikeSource()).name(QueryLikeSource.DESCRIPTION);

        DataStream<EntryFind> records = queries.flatMap(new DBFetcherCallBack()).rebalance()
            .name(DBFetcherCallBack.DESCRIPTION);

        DataStream<String> jsonRecords = records.map(new JsonTextParser<EntryFind>()).name(JsonTextParser.DESCRIPTION);

        /*
         * Is this rebalance REALLY important???
         */
        DataStream<KbwcEntryDocument> documents = jsonRecords.map(new DocumentParser()).rebalance()
            .name(DocumentParser.DESCRIPTION);

        /*
         * Windows can be defined on already partitioned KeyedStreams. Windows group the data in each key according to
         * some characteristic (e.g., the data that arrived within the last 5 seconds).
         */
        /*
         * Windows group all the stream events according to some
         * characteristic (e.g., the data that arrived within the last 5 seconds).
         * WARNING: This is in many cases a non-parallel transformation. All records will be gathered in one task for
         * the windowAll operator.
         */
        /*
         * Windows can be defined on regular (non-keyed) data streams using the windowAll transformation and grouping
         * all the stream events according to some characteristic (e.g., the data that arrived within the last 5
         * seconds). These windowed data streams have all the capabilities of keyed windowed data streams, BUT are
         * evaluated at a SINGLE TASK (and hence at a single computing node).
         */
        /*
         * Is this rebalance REALLY important???
         */
        DataStream<List<KbwcEntryDocument>> windowed = documents
            .keyBy(new SolrKeySelector<KbwcEntryDocument, Integer>())
            .timeWindow(Time.milliseconds(1000))
            .apply(new SolrTimeWindow<KbwcEntryDocument, List<KbwcEntryDocument>, Integer, TimeWindow>())
            .rebalance()
            .name(SolrTimeWindow.DESCRIPTION);

        windowed.addSink(new SolrSink<List<KbwcEntryDocument>>(configMap)).name(SolrSink.DESCRIPTION);;

        env.execute("Receives SQL queries... executes them and then writes to Solr");
    }

    /**
     *
     */
    public class RecordCounter extends RichMapFunction<KbwcEntryDocument, KbwcEntryDocument> {
        private static final long serialVersionUID = 1L;
        /**
         * Concise description of what this class represents.
         */
        public static final String DESCRIPTION = "Counts records";
        private LongCounter recordCount = new LongCounter();

        @Override
        public void open(final Configuration parameters) throws Exception {
            super.open(parameters);
            getRuntimeContext().addAccumulator("recordCount", recordCount);
        }

        @Override
        public KbwcEntryDocument map(final KbwcEntryDocument obj) throws Exception {
            recordCount.add(1L);
            return obj;
        }
    }

    /**
     * @param <IN>
     * @param <OUT>
     */
    public class SolrKeySelector<IN, OUT> implements KeySelector<KbwcEntryDocument, OUT> {
        private static final long serialVersionUID = 1L;
        /**
         * Concise description of what this class does.
         */
        public static final String DESCRIPTION = "Selects a key from the the document";

        @SuppressWarnings("unchecked")
        @Override
        public OUT getKey(final KbwcEntryDocument document) throws Exception {
            //return document.getCollection();
            return (OUT) document.getOwnerInstitution();
        }
    }

    /**
     * @param <IN>
     * @param <OUT>
     * @param <KEY>
     * @param <WINDOW>
     */
    public class SolrTimeWindow<IN, OUT, KEY, WINDOW> implements
        WindowFunction<KbwcEntryDocument, List<KbwcEntryDocument>, Integer, TimeWindow> {
        private static final long serialVersionUID = 1L;
        /**
         * Concise description of what this class does.
         */
        public static final String DESCRIPTION = "Windows elements into a window, based on a key";

        @Override
        public void apply(final Integer key, final TimeWindow window, final Iterable<KbwcEntryDocument> values,
            final Collector<List<KbwcEntryDocument>> collector) throws Exception {

            List<KbwcEntryDocument> list = new ArrayList<KbwcEntryDocument>();
            for (KbwcEntryDocument document : values) {
                list.add(document);
            }
            
            collector.collect(list);
        }
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        System.setProperty("environment", "test");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        QueryStreamToDbToSolrJob job = new QueryStreamToDbToSolrJob();
        job.init();
        job.execute(env);
    }
}
