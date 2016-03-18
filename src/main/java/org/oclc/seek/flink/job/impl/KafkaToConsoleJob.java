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
import org.oclc.seek.flink.source.KafkaSource;

/**
 *
 */
public class KafkaToConsoleJob extends JobGeneric {
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

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        DataStream<String> events = env.addSource(new KafkaSource(parameterTool.getRequired("db.table"),
            parameterTool.getProperties()).getSource()).name(KafkaSource.DESCRIPTION);

        // write kafka events to standard out.
        events.print();

        env.execute("Read from Kafka and print out");
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        System.setProperty("environment", "test");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaToConsoleJob kc = new KafkaToConsoleJob();
        kc.execute(env);
    }
}
