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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction.WatchType;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.oclc.seek.flink.job.JobContract;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.stream.sink.KafkaSinkBuilder;

/**
 *
 */
public class HdfsToKafka extends JobGeneric implements JobContract {
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

        parameterTool = ParameterTool.fromMap(propertiesToMap(props));
    }

    @Override
    public void execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // env.getConfig().disableSysoutLogging();

        // use system default value
        env.getConfig().setNumberOfExecutionRetries(-1);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        // env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));

        // create a checkpoint every 5 secodns
        env.enableCheckpointing(5000);

        env.readFileStream("hdfs:///" + parameterTool.get("hdfs.folder") + "/result", 5000, WatchType.ONLY_NEW_FILES)
        .map(new MapFunction<String, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String map(final String value) throws Exception {
                return value;
            }
        }).name("json-records")
        .addSink(new KafkaSinkBuilder().build(parameterTool.get("kafka.topic"), parameterTool.getProperties()))
        .name("kafka");

        env.execute("Generates Queries... executes them and writes results to HDFS");
    }

}
