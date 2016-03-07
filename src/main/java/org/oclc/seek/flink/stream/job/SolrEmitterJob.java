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
import org.oclc.seek.flink.record.DbInputRecord;
import org.oclc.seek.flink.record.DbInputRecordBuilder;
import org.oclc.seek.flink.stream.function.SolrSink;

/**
 *
 */
public class SolrEmitterJob extends JobGeneric implements JobContract {
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
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        // create a checkpoint every 5 secodns
        // env.enableCheckpointing(5000);

        // HttpSolrClient solrClient = (HttpSolrClient) getSolrClient();
        Map<String, String> config = new HashMap<String, String>();

        config.put("solr.type", "Http");
        // config.put("solr.location", solrClient.getBaseURL());
        config.put("zkconnectionstring", parameterTool.getRequired("zookeeper.connect"));

        // DataStream<String> text = env.readTextFile(parameterTool.getRequired("hdfs.kafka.source"));
        // DataStream<String> text =
        // env.readFileStream(parameterTool.getRequired("hdfs.solr.source"), 1000, WatchType.ONLY_NEW_FILES);

        // Streams json records every 10 ms
        DataStream<DbInputRecord> text = env.addSource(new SimpleStringGenerator());

        //
        DataStream<String> jsonRecords = text.map(new RichMapFunction<DbInputRecord, String>() {
            private static final long serialVersionUID = 1L;
            private LongCounter recordCount = new LongCounter();

            @Override
            public void open(final Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("recordCount", recordCount);
            }

            @Override
            public String map(final DbInputRecord record) throws Exception {
                recordCount.add(1L);
                return record.toJson();
            }
        }).name("json-records");

        jsonRecords.addSink(new SolrSink<String>(config, new SolrDocumentBuilder()))
        .name("solr");

        env.execute("Writes json records to Solr");
    }

    /**
     *
     */
    public static class SimpleStringGenerator implements SourceFunction<DbInputRecord> {
        private static final long serialVersionUID = 2174904787118597072L;
        boolean running = true;
        long i = 1;

        @Override
        public void run(final SourceContext<DbInputRecord> ctx) throws Exception {
            while (running && i <= 100) {
                ctx.collect(new DbInputRecordBuilder().ownerInstitution(91475L + i++)
                    .collectionUid("wiley.interScience")
                    .build());
                Thread.sleep(10);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
