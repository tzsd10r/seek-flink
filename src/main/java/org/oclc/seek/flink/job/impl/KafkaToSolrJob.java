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
import org.oclc.seek.flink.mapper.JsonToDocumentTransformer;
import org.oclc.seek.flink.sink.SolrSink;
import org.oclc.seek.flink.source.KafkaSource;

/**
 *
 */
public class KafkaToSolrJob extends JobGeneric {
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
        // make parameters available everywhere
        env.getConfig().setGlobalJobParameters(parameterTool);

        String zkHosts = parameterTool.getRequired(SolrSink.ZKHOSTS);
        String collection = parameterTool.getRequired(SolrSink.COLLECTION);
        Map<String, String> configMap = ImmutableMap
            .of(SolrSink.ZKHOSTS, zkHosts, SolrSink.COLLECTION, collection);

        String suffix = parameterTool.getRequired("db.table");

        DataStream<String> jsonRecords =
            env.addSource(new KafkaSource(suffix, parameterTool.getProperties()).getSource())
            .name(KafkaSource.DESCRIPTION);

        DataStream<KbwcEntryDocument> documents = jsonRecords.map(new JsonToDocumentTransformer())
            .name(JsonToDocumentTransformer.DESCRIPTION);
        
        DataStream<List<KbwcEntryDocument>> windowed = documents
            .keyBy(new SolrKeySelector<KbwcEntryDocument, Object>()).timeWindow(Time.seconds(1))
            .apply(new SolrTimeWindowList<KbwcEntryDocument, List<KbwcEntryDocument>, Object, TimeWindow>())
            .rebalance().name(SolrTimeWindowList.DESCRIPTION);

        windowed.map(new SolrRecordCounter()).addSink(new SolrSink<List<KbwcEntryDocument>>(configMap))
            .name(SolrSink.DESCRIPTION);

//        documents.addSink(new SolrSink<KbwcEntryDocument>(configMap))
//        .name(SolrSink.DESCRIPTION);

        env.execute("Reads from Kafka and indexes to Solr");
    }
    
    /**
    *
    */
   public class SolrRecordCounter extends RichMapFunction<List<KbwcEntryDocument>, List<KbwcEntryDocument>> {
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
}
