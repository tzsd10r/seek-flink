/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.batch.job;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.oclc.seek.flink.job.JobContract;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.record.DatabaseInputRecord;

/**
 *
 */
public class DBImportHadoopToHdfsJob extends JobGeneric implements JobContract {
    private Properties props = new Properties();

    @Override
    public void init(final String query) {
        props.put("query", query);
    }

    @Override
    public void init() {
        String env = System.getProperty("environment");
        String test = System.getProperty("test");

        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader)cl).getURLs();

        for(URL url: urls){
            System.out.println(url.getFile());
        }

        String configFile = "conf/config." + env + ".properties";

        if (test != null) {
            configFile = "config.test.properties";
        }

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
    public void execute() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ExecutionConfig config = env.getConfig();

        System.out.println("Configuration...\n" + config);
        System.out.println("Configuration...\n" + config.toString());

        // make parameters available in the web interface
        config.setGlobalJobParameters(parameterTool);

        JobConf conf = new JobConf();

        DBConfiguration.configureDB(conf,
            parameterTool.getRequired("db.driver"),
            parameterTool.getRequired("db.url"),
            parameterTool.getRequired("db.user"),
            parameterTool.getRequired("db.password"));

        DBInputFormat.setInput(conf,
            DatabaseInputRecord.class,
            parameterTool.getRequired("db.table"),
            null,
            null,
            new String[] {
            parameterTool.getRequired("db.fields")
        });

        HadoopInputFormat<LongWritable, DatabaseInputRecord> hadoopInputFormat =
            new HadoopInputFormat<LongWritable, DatabaseInputRecord>(
                new DBInputFormat(), LongWritable.class, DatabaseInputRecord.class, conf);

        conf.setStrings("mapred.jdbc.input.count.query", "select count(*) from entry_find");
        conf.setStrings("mapreduce.jdbc.input.count.query", "select count(*) from entry_find");
        conf.setNumTasksToExecutePerJvm(1);
        conf.setNumMapTasks(parameterTool.getInt("map.tasks", 5));
        conf.writeXml(System.out);


        /*
         * get records from database
         */
        DataSet<String> records = env
            .createInput(hadoopInputFormat)
            .map(new RichMapFunction<Tuple2<LongWritable, DatabaseInputRecord>, String>() {
                private static final long serialVersionUID = 1L;
                private LongCounter recordCount = new LongCounter();

                @Override
                public void open(final Configuration parameters) throws Exception {
                    super.open(parameters);
                    getRuntimeContext().addAccumulator("recordCount", recordCount);
                }

                @Override
                public String map(final Tuple2<LongWritable, DatabaseInputRecord> tuple) throws Exception {
                    recordCount.add(1L);
                    DatabaseInputRecord dbInputRecord = tuple.f1;
                    return dbInputRecord.toJson();
                }
            })
            .name("build db record")
            .returns(String.class).rebalance();

        /*
         * send records to hdfs
         */
        records
        .writeAsText("hdfs:///" + parameterTool.get("hdfs.folder") + "/result", WriteMode.OVERWRITE)
        .name("hdfs");

        // Setup Hadoop’s TextOutputFormat
        // HadoopOutputFormat<Text, LongWritable> hadoopOutputFormat =
        // new HadoopOutputFormat<Text, LongWritable>(
        // new TextOutputFormat<Text, LongWritable>(), new JobConf());
        // hadoopOutputFormat.getJobConf().set("mapred.textoutputformat.separator", " ");

        // FileOutputFormat.setOutputPath(hadoopOutputFormat.getJobConf(), new Path(outputPath));

        JobExecutionResult result = env.execute("Fetch data from database and store on HDFS");
        long rc = result.getAccumulatorResult("recordCount");

        System.out.println("recordCount: " + rc);
    }
}
