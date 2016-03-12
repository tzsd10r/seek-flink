/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.impl;

import java.util.Map;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.com.google.common.collect.ImmutableMap;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.oclc.seek.flink.builder.DocumentBuilder;
import org.oclc.seek.flink.document.KbwcEntryDocument;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.record.EntryFind;
import org.oclc.seek.flink.sink.SolrSink;
import org.oclc.seek.flink.sink.SolrSinkBuilder;
import org.oclc.seek.flink.source.KafkaSourceBuilder;

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
        // create a checkpoint every 5 seconds
        // env.enableCheckpointing(5000);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        String zkHosts = parameterTool.getRequired(SolrSink.ZKHOSTS);
        String collection = parameterTool.getRequired(SolrSink.COLLECTION);
        Map<String, String> configMap = ImmutableMap
            .of(SolrSink.ZKHOSTS, zkHosts, SolrSink.COLLECTION, collection);

        DataStream<String> jsonRecords = env.addSource(new KafkaSourceBuilder().build(
            parameterTool.get("kafka.src.topic." + parameterTool.getRequired("db.table")),
            parameterTool.getProperties())).name("Listens to json records from Kafka");

        // DataStream<String> jsonRecords = env.addSource(new KafkaSourceBuilder().build(
        // Config.TOPIC_INDEX_INPUT,
        // Config.PROP_KAFKA));

        // Define the desired time window
        // WindowedStream<T, K, Window>

        DataStream<KbwcEntryDocument> docs = jsonRecords
            .map(new RichMapFunction<String, KbwcEntryDocument>() {
                private static final long serialVersionUID = 1L;
                private LongCounter recordCount = new LongCounter();
                private DocumentBuilder<KbwcEntryDocument> builder;

                @Override
                public void open(final Configuration parameters) throws Exception {
                    super.open(parameters);
                    getRuntimeContext().addAccumulator("recordCount", recordCount);
                    builder = new DocumentBuilder<KbwcEntryDocument>();
                }

                @Override
                public KbwcEntryDocument map(final String json) throws Exception {
                    EntryFind entryFind = new EntryFind().fromJson(json);
                    recordCount.add(1L);
                    return builder.build(entryFind, KbwcEntryDocument.class);
                }
            });// .name("convert json into documents and count the records");

        docs.addSink(new SolrSinkBuilder<KbwcEntryDocument>().build(configMap));
        // .name("Index to solr sink");;

        env.execute("Reads from Kafka and indexes to Solr");
    }
}
