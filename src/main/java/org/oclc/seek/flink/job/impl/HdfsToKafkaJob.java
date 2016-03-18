/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.impl;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction.WatchType;
import org.oclc.seek.flink.function.CountRecords;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.sink.KafkaSinkBuilder;

/**
 *
 */
public class HdfsToKafkaJob extends JobGeneric {
    private static final long serialVersionUID = 1L;

    @Override
    public void init() {
        super.init();
    }

    @Override
    public void execute(final StreamExecutionEnvironment env) throws Exception {
        // create a checkpoint every 5 secodns
        // env.enableCheckpointing(5000);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        String suffix = parameterTool.getRequired("db.table");

        String path = parameterTool.getRequired("fs.src.dir." + suffix) + "/2016-03-17";

        DataStream<String> jsonRecords = env.readFileStream(path, 100, WatchType.ONLY_NEW_FILES)
            .map(new CountRecords<String>());

        // jsonRecords.map(new RichMapFunction<String, String>() {
        // private static final long serialVersionUID = 1L;
        // private LongCounter recordCount = new LongCounter();
        //
        // @Override
        // public void open(final Configuration parameters) throws Exception {
        // super.open(parameters);
        // getRuntimeContext().addAccumulator("recordCount", recordCount);
        // }
        //
        // @Override
        // public String map(final String value) throws Exception {
        // recordCount.add(1L);
        // return value;
        // }
        // }).name("json-records")

        String topic = parameterTool.get("kafka.sink.topic." + suffix);

        jsonRecords.addSink(new KafkaSinkBuilder().build(topic, parameterTool.getProperties()))
        .name("write json records to kafka");

        env.execute("Reads from HDFS and writes to Kafka");
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
        HdfsToKafkaJob job = new HdfsToKafkaJob();
        job.init();
        job.execute(env);
    }
}
