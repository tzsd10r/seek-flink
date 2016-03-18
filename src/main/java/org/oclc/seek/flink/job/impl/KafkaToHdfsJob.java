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
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.sink.HdfsSinkBuilder;
import org.oclc.seek.flink.source.KafkaSource;

/**
 *
 */
public class KafkaToHdfsJob extends JobGeneric {
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
        env.enableCheckpointing(5000);

        // defines how many times the job is restarted after a failure
        // env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 60000));

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        String suffix = parameterTool.getRequired("db.table");

        DataStream<String> events = env.addSource(new KafkaSource(suffix, parameterTool.getProperties()).getSource())
            .name(KafkaSource.DESCRIPTION);

        events.addSink(new HdfsSinkBuilder(suffix, parameterTool.getProperties()).getSink())
        .name(HdfsSinkBuilder.DESCRIPTION);

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
