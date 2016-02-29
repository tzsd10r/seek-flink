/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.stream.job;

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
        props.put("group.id", "seek-kafka");
        props.put("zookeeper.connect",
            "ilabhddb03dxdu.dev.oclc.org:9011,ilabhddb04dxdu.dev.oclc.org:9011,ilabhddb05dxdu.dev.oclc.org:9011");
        props.put("kafka.topic", "test-events");
        props.put("bootstrap.servers", "ilabhddb03dxdu:9077,ilabhddb04dxdu:9077,ilabhddb05dxdu:9077");
        props.put("hdfs.folder", "/user/seabrae/flink");
        props.put("hdfs.host", "hdfs://ilabhddb02dxdu.dev.oclc.org:9008");

        parameterTool = ParameterTool.fromMap(propertiesToMap(props));
    }

    /**
     * @param configFile
     * @throws Exception
     */
    public KafkaToHdfsJob(final String configFile) throws Exception {
        Properties props = new Properties();
        try {
            props.load(KafkaToHdfsJob.class.getResourceAsStream(configFile));
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(0);
        }

        parameterTool = ParameterTool.fromMap(propertiesToMap(props));
    }

    /**
     * @throws Exception
     */
    @Override
    public void execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.getConfig().disableSysoutLogging();
        // use system default value
        env.getConfig().setNumberOfExecutionRetries(-1);
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);
        // env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));

        env.enableCheckpointing(5000); // create a checkpoint every 5 secodns

        KafkaSourceBuilder kafkaSourceBuilder = new KafkaSourceBuilder();
        HdfsSink hdfsSink = new HdfsSink();

        FlinkKafkaConsumer082<String> source = kafkaSourceBuilder.build(parameterTool.getRequired("kafka.topic"),
            parameterTool.getProperties());

        SinkFunction<String> sink = hdfsSink.build(parameterTool.get("hdfs.folder"));

        DataStreamSource<String> stream = env.addSource(source, "kafka");
        stream.addSink(sink).name("hdfs");

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
            configFile = "config/config.prod.properties";
            System.out.println("Missing input : config file location, using default: " + configFile);
        } else {
            configFile = args[0];
        }

        KafkaToHdfsJob kh = new KafkaToHdfsJob(configFile);
        kh.execute();
    }

}
