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
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.oclc.seek.flink.document.KbwcEntryDocument;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.mapper.DBFetcherCallBack2;
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
        //DataStream<String> queries = env.addSource(new QueryOffsetSource(env.getParallelism()))
        //    .name(QueryOffsetSource.DESCRIPTION);

        /*
         * Is this rebalance REALLY important here?? NO... actually... it is better w/o, because the rebalance always
         * has an impact on performance.
         */
        DataStream<EntryFind> records = queries.flatMap(new DBFetcherCallBack2())
        // .assignTimestamps(new ReadingsTimestampAssigner())
            .name(DBFetcherCallBack2.DESCRIPTION);
        
        DataStream<String> jsonRecords = records.map(new JsonTextParser<EntryFind>()).name(JsonTextParser.DESCRIPTION);

        /*
         * Is this rebalance REALLY important here??? NO... actually... it is better w/o, because the rebalance always
         * has an impact on performance.
         */
        DataStream<KbwcEntryDocument> documents = jsonRecords.map(new DocumentParser())
            .name(DocumentParser.DESCRIPTION);

//        if (parameterTool.getRequired("with").equalsIgnoreCase("1")) {
//        } else {
//        }

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
         * Is this rebalance REALLY important??? YES. The rebalance distributes the work load more appropriatedly across
         * the workers emitting to Solr.
         * 
         * What is the best time window? The smaller the window... the better... in this case, 1 second is good, which
         * pushes about 30K elements at a time... and the total time is about 15-20 mins less when compared to the
         * 2 second window
         */
        DataStream<List<KbwcEntryDocument>> windowed = documents
            .keyBy(new SolrKeySelector<KbwcEntryDocument, Object>()).timeWindow(Time.seconds(1))
            .apply(new SolrTimeWindowList<KbwcEntryDocument, List<KbwcEntryDocument>, Object, TimeWindow>())
            .rebalance().name(SolrTimeWindowList.DESCRIPTION);

        windowed.map(new RecordCounter()).addSink(new SolrSink<List<KbwcEntryDocument>>(configMap))
            .name(SolrSink.DESCRIPTION);

        env.execute("Receives SQL queries... executes them and then writes to Solr");
    }

    /**
     *
     */
    public class RecordCounter extends RichMapFunction<List<KbwcEntryDocument>, List<KbwcEntryDocument>> {
        private static final long serialVersionUID = 1L;
        /**
         * Concise description of what this class represents.
         */
        public static final String DESCRIPTION = "Counts elements being processed";
        private LongCounter recordCount = new LongCounter();

        @Override
        public void open(final Configuration parameters) throws Exception {
            super.open(parameters);
            getRuntimeContext().addAccumulator("recordCount", recordCount);
        }

        @Override
        public List<KbwcEntryDocument> map(final List<KbwcEntryDocument> list) throws Exception {
            recordCount.add(list.size());
            return list;
        }
    }

    /**
     * @param <IN>
     * @param <OUT>
     */
    public class SolrKeySelector<IN, OUT> implements KeySelector<KbwcEntryDocument, Object> {
        private static final long serialVersionUID = 1L;
        /**
         * Concise description of what this class does.
         */
        public static final String DESCRIPTION = "Selects a key from the the document";

        @Override
        public Object getKey(final KbwcEntryDocument document) throws Exception {
            return document.getCollection();
        }
    }

    /**
     * @param <IN>
     * @param <OUT>
     * @param <KEY>
     * @param <WINDOW>
     */
    public class SolrTimeWindowList<IN, OUT, KEY, WINDOW> implements
        WindowFunction<KbwcEntryDocument, List<KbwcEntryDocument>, Object, TimeWindow> {
        private static final long serialVersionUID = 1L;
        /**
         * Concise description of what this class does.
         */
        public static final String DESCRIPTION = "Windows elements into a window, based on a key";

        @Override
        public void apply(final Object key, final TimeWindow window, final Iterable<KbwcEntryDocument> values,
            final Collector<List<KbwcEntryDocument>> collector) throws Exception {

            /*
             * Batching documents as an List... this is way faster than actually collecting each document at a time.
             */
            List<KbwcEntryDocument> list = new ArrayList<KbwcEntryDocument>();
            for (KbwcEntryDocument document : values) {
                list.add(document);
            }
            collector.collect(list);
        }
    }

    public class ReadingsTimestampAssigner implements TimestampExtractor<EntryFind> {
        private static final long serialVersionUID = 1L;
        /**
         * in milliseconds
         */
        private static final long MAX_DELAY_MS = 12000;
        private long maxTimestamp;

        @Override
        public long extractTimestamp(EntryFind element, long currentTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.timestamp());
            return element.timestamp();
        }

        @Override
        public long extractWatermark(EntryFind element, long currentTimestamp) {
            return Long.MIN_VALUE;
        }

        @Override
        public long getCurrentWatermark() {
            return maxTimestamp - MAX_DELAY_MS;
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
