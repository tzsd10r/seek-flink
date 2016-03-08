/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.stream.job;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.oclc.seek.flink.batch.document.SolrDocumentBuilder;
import org.oclc.seek.flink.job.JobContract;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.stream.function.SolrSink;
import org.oclc.seek.flink.stream.source.KafkaSourceBuilder;

/**
 *
 */
public class KafkaToSolrJob extends JobGeneric implements JobContract {
    private Properties props = new Properties();

    @Override
    public void init() {
        String env = System.getProperty("environment");

        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader) cl).getURLs();

        for (URL url : urls) {
            System.out.println(url.getFile());
        }

        String configFile = "conf/config." + env + ".properties";

        // Properties properties = new Properties();
        try {
            props.load(ClassLoader.getSystemResourceAsStream(configFile));
        } catch (Exception e) {
            System.out.println("Failed to load the properties file... [" + configFile + "]");
            e.printStackTrace();
            throw new RuntimeException("Failed to load the properties file... [" + configFile + "]");
        }

        // String solrXml = "solr.xml";

        parameterTool = ParameterTool.fromMap(propertiesToMap(props));
    }

    @Override
    public void execute(final StreamExecutionEnvironment env) throws Exception {
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        // create a checkpoint every 5 secodns
        env.enableCheckpointing(5000);

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
