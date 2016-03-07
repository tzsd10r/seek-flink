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
package org.oclc.seek.flink.stream.job;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.oclc.seek.flink.job.JobContract;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.record.DbInputRecord;
import org.oclc.seek.flink.stream.sink.KafkaSinkBuilder;

/**
 *
 */
public class DbToKafkaJob extends JobGeneric implements JobContract {
    private Properties props = new Properties();

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

    @Override
    public void execute(final StreamExecutionEnvironment env) throws Exception {
        // env.getConfig().disableSysoutLogging();

        // create a checkpoint every 1000 ms
        env.enableCheckpointing(1000);
        // set the timeout low to minimize latency
        env.setBufferTimeout(10);

        ExecutionConfig config = env.getConfig();

        // use system default value
        config.setNumberOfExecutionRetries(-1);

        // make parameters available in the web interface
        config.setGlobalJobParameters(parameterTool);

        // defines how many times the job is restarted after a failure
        config.setNumberOfExecutionRetries(5);

        JobConf conf = new JobConf();

        DBConfiguration.configureDB(conf,
            parameterTool.getRequired("db.driver"),
            parameterTool.getRequired("db.url"),
            parameterTool.getRequired("db.user"),
            parameterTool.getRequired("db.password"));

        DBInputFormat.setInput(conf,
            DbInputRecord.class,
            parameterTool.getRequired("db.table"),
            null,
            null,
            new String[] {
            parameterTool.getRequired("db.fields")
        });

        HadoopInputFormat<LongWritable, DbInputRecord> hadoopInputFormat =
            new HadoopInputFormat<LongWritable, DbInputRecord>(
                new DBInputFormat(), LongWritable.class, DbInputRecord.class, conf);

        // conf.setStrings("mapred.jdbc.input.count.query", "select count(*) from entry_find");
        // conf.setStrings("mapreduce.jdbc.input.count.query", "select count(*) from entry_find");
        // conf.setNumTasksToExecutePerJvm(1);

        conf.setNumMapTasks(parameterTool.getInt("map.tasks", 6));

        DataStream<Tuple2<LongWritable, DbInputRecord>> rawRecords =
            env.createInput(hadoopInputFormat).name("db source");

        DataStream<String> jsonRecords =
            rawRecords
            .map(new RichMapFunction<Tuple2<LongWritable, DbInputRecord>, String>() {
                private static final long serialVersionUID = 1L;
                private LongCounter recordCount = new LongCounter();

                @Override
                public void open(final Configuration parameters) throws Exception {
                    super.open(parameters);
                    getRuntimeContext().addAccumulator("recordCount", recordCount);
                }

                @Override
                public String map(final Tuple2<LongWritable, DbInputRecord> tuple) throws Exception {
                    recordCount.add(1L);
                    DbInputRecord dbInputRecord = tuple.f1;
                    return dbInputRecord.toJson();
                }
            }).name("convert db record into json");

        DataStreamSink<String> kafka = jsonRecords.addSink(
            new KafkaSinkBuilder().build(
                parameterTool.get(parameterTool.getRequired("db.table") + ".kafka.sink.topic"),
                parameterTool.getProperties()))
                .name("put json records on Kafka");

        // DataStreamSink<String> filesystem = jsonRecords.addSink(
        // new HdfsSink().build(parameterTool.get(parameterTool.getRequired("db.table") + ".fs.sink.dir")))
        // .name("put json records on filesystem");

        env.execute("Queries the DB and drops results on Kafka");
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        System.setProperty("environment", "test");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DbToKafkaJob job = new DbToKafkaJob();
        job.init();
        job.execute(env);
    }
}
