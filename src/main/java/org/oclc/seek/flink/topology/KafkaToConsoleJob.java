/****************************************************************************************************************
 *
 *  Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 *
 *  OCLC proprietary information: the enclosed materials contain
 *  proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 *  any part to any third party or used by any person for any purpose, without written
 *  consent of OCLC, Inc.  Duplication of any portion of these  materials shall include his notice.
 *
 ******************************************************************************************************************/
package org.oclc.seek.flink.topology;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.oclc.seek.flink.job.JobContract;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.source.KafkaSourceBuilder;

/**
 *
 */
public class KafkaToConsoleJob extends JobGeneric implements JobContract {
    @Override
    public void init() {
        super.init();
    }

    /**
     * @param configFile
     * @throws Exception
     */
    public KafkaToConsoleJob(final String configFile) throws Exception {
        Properties configs = new Properties();
        try {
            configs.load(ClassLoader.getSystemResourceAsStream(configFile));
            Map<String, String> map = new HashMap<String, String>();
            for (String key : configs.stringPropertyNames()) {
                map.put(key, configs.getProperty(key));
            }

            parameterTool = ParameterTool.fromMap(map);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(0);
        }
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


        DataStream<String> stream = env.addSource(new KafkaSourceBuilder().build(
            parameterTool.getRequired(parameterTool.getRequired("db.table") + ".kafka.src.topic"),
            parameterTool.getProperties()), "kafka source");

        // write kafka stream to standard out.
        stream.print();

        System.out.println(env.getExecutionPlan());

        env.execute("Read from Kafka example and print out");
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        String configFile;
        if (args.length == 0) {
            configFile = "conf/conf.local.properties";
            System.out.println("Missing input : conf file location, using default: " + configFile);
        } else {
            configFile = args[0];
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaToConsoleJob kc = new KafkaToConsoleJob(configFile);
        kc.execute(env);
    }
}
