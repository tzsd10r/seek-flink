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
import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.oclc.seek.flink.job.JobContract;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.stream.sink.HdfsSink;
import org.oclc.seek.flink.stream.source.KafkaSourceBuilder;

/**
 *
 */
public class KafkaToHdfsJob extends JobGeneric implements JobContract {
    Properties props = new Properties();

    @Override
    public void init() {
        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader) cl).getURLs();

        for (URL url : urls) {
            System.out.println(url.getFile());
        }

        String env = System.getProperty("environment");
        String configFile = "conf/config." + env + ".properties";

        System.out.println("Using this config file... [" + configFile + "]");

        try {
            props.load(ClassLoader.getSystemResourceAsStream(configFile));
        } catch (Exception e) {
            System.out.println("Failed to load the properties file... [" + configFile + "]");
            e.printStackTrace();
            throw new RuntimeException("Failed to load the properties file... [" + configFile + "]");
        }

        parameterTool = ParameterTool.fromMap(propertiesToMap(props));
    }

    /**
     * @throws Exception
     */
    @Override
    public void execute(final StreamExecutionEnvironment env) throws Exception {
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.getConfig().disableSysoutLogging();
        // use system default value
        env.getConfig().setNumberOfExecutionRetries(5);
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);
        // env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));

        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds

        KafkaSourceBuilder kafkaSourceBuilder = new KafkaSourceBuilder();
        HdfsSink hdfsSink = new HdfsSink();

        FlinkKafkaConsumer082<String> source =
            kafkaSourceBuilder.build(
                parameterTool.getRequired(parameterTool.getRequired("db.table") + ".kafka.src.topic"),
                parameterTool.getProperties());

        SinkFunction<String> sink =
            hdfsSink.build(parameterTool.get(parameterTool.getRequired("db.table") + ".fs.stage.dir"));

        DataStreamSource<String> stream = env.addSource(source, "kafka source");
        stream.addSink(sink).name("filesystem sink");

        // write kafka stream to standard out.
        // messageStream.print();

        // System.out.println(env.getExecutionPlan());
        env.execute("Read Events from Kafka and write to HDFS");
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        String configFile;
        if (args.length == 0) {
            configFile = "conf/conf.prod.properties";
            System.out.println("Missing input : conf file location, using default: " + configFile);
        } else {
            configFile = args[0];
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaToHdfsJob job = new KafkaToHdfsJob();
        job.init();
        job.execute(env);
    }

}
