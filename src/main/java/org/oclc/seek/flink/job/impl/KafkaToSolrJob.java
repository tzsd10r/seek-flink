/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.oclc.seek.flink.document.SolrDocumentBuilder;
import org.oclc.seek.flink.function.SolrSink;
import org.oclc.seek.flink.job.JobContract;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.source.KafkaSourceBuilder;

/**
 *
 */
public class KafkaToSolrJob extends JobGeneric implements JobContract {

    @Override
    public void init() {
        super.init();
    }

    @Override
    public void execute(final StreamExecutionEnvironment env) throws Exception {
        // create a checkpoint every 5 secodns
        env.enableCheckpointing(5000);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        // defines how many times the job is restarted after a failure
        // env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 60000));

        Map<String, String> config = new HashMap<String, String>();
        config.put(SolrSink.SOLR_ZK_STRING, parameterTool.getRequired(SolrSink.SOLR_ZK_STRING));

        /*
         * Kafka streaming source
         */
        SourceFunction<String> source =
            new KafkaSourceBuilder().build(
                parameterTool.get(parameterTool.getRequired("db.table") + ".kafka.src.topic"),
                parameterTool.getProperties());

        DataStream<String> jsonRecords = env
            .addSource(source)
            .name("kafka source")
            .rebalance();

        jsonRecords.map(new RichMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            private LongCounter recordCount = new LongCounter();

            @Override
            public void open(final Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("recordCount", recordCount);
            }

            @Override
            public String map(final String record) throws Exception {
                recordCount.add(1L);
                return record;
            }
        }).name("No transformation... just count the records");

        jsonRecords.addSink(new SolrSink<String>(config, new SolrDocumentBuilder()))
        .name("solr sink");

        env.execute("Fetches json records from Kafka and emits them to Solr");
    }
}
