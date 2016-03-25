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
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.mapper.RecordCounter;
import org.oclc.seek.flink.sink.KafkaSink;

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
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        String suffix = parameterTool.getRequired("db.table");

        String path = parameterTool.getRequired("fs.src.dir." + suffix);

        DataStream<String> jsonRecords = env.readFileStream(path, 100, WatchType.ONLY_NEW_FILES)
            .map(new RecordCounter<String>()).name(RecordCounter.DESCRIPTION);

        jsonRecords.addSink(new KafkaSink(suffix, parameterTool.getProperties()).getSink()).name(KafkaSink.DESCRIPTION);

        env.execute("Reads from HDFS and writes to Kafka");
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        System.setProperty("environment", "test");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        HdfsToKafkaJob job = new HdfsToKafkaJob();
        job.init();
        job.execute(env);
    }
}
