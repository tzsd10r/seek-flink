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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
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
public class DBImportHadoopJob extends JobGeneric implements JobContract {
    private Properties props = new Properties();

    @Override
    public void init(final String query) {
        props.put("query", query);
    }

    /**
     *
     */
    @Override
    public void init() {
        String env = System.getProperty("environment");

        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader)cl).getURLs();

        for(URL url: urls){
            System.out.println(url.getFile());
        }

        // Properties properties = new Properties();
        try {
            props.load(ClassLoader.getSystemResourceAsStream("config/config." + env + ".properties"));
        } catch (Exception e) {
            System.out.println("Failed to load the properties file");
            e.printStackTrace();
            throw new RuntimeException("Failed to load the properties file");
        }

        // props.put("zookeeper.connect",
        // "ilabhddb03dxdu.dev.oclc.org:9011,ilabhddb04dxdu.dev.oclc.org:9011,ilabhddb05dxdu.dev.oclc.org:9011");
        // props.put("hdfs.folder", "/user/seabrae/flink");
        // props.put("hdfs.host", "hdfs://ilabhddb02dxdu.dev.oclc.org:9008");
        //
        // props.put("driver", "com.mysql.jdbc.Driver");
        // props.put("table", "entry_find");
        // props.put("fields", "owner_institution, collection_uid");
        // props.put("url", "jdbc:mysql://wmsdev-vip3.ent.oclc.org:3306/kbwc_seek");
        // props.put("user", "kbwc_seek");
        // props.put("password", "kbwc_seek");
        // props.put("conditions", "limit 100");

        parameterTool = ParameterTool.fromMap(propertiesToMap(props));
    }

    @Override
    public void execute() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ExecutionConfig config = env.getConfig();

        System.out.println("Configuration...\n" + config);
        System.out.println("Configuration...\n" + config.toString());

        // env.getConfig().disableSysoutLogging();
        // use system default value
        config.setNumberOfExecutionRetries(-1);
        // make parameters available in the web interface
        config.setGlobalJobParameters(parameterTool);
        // env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));

        // env.enableCheckpointing(5000); // create a checkpoint every 5 seconds


        JobConf conf = new JobConf();

        DBConfiguration.configureDB(conf,
            parameterTool.getRequired("driver"),
            parameterTool.getRequired("url"),
            parameterTool.getRequired("user"),
            parameterTool.getRequired("password"));

        System.out.println("Job Conf...\n" + conf);

        DBInputFormat.setInput(conf,
            DatabaseInputRecord.class,
            parameterTool.getRequired("table"),
            null,
            null,
            new String[] {
            parameterTool.getRequired("fields")
        });

        System.out.println("Job Conf...\n" + conf);

        HadoopInputFormat<LongWritable, DatabaseInputRecord> hadoopInputFormat =
            new HadoopInputFormat<LongWritable, DatabaseInputRecord>(
                new DBInputFormat(), LongWritable.class, DatabaseInputRecord.class, conf);

        System.out.println("Job Conf...\n" + conf);

        /*
         * get records from database
         */
        DataSet<String> records = env
            .createInput(hadoopInputFormat)
            .map(new MapFunction<Tuple2<LongWritable, DatabaseInputRecord>, String>() {
                private static final long serialVersionUID = 1L;

                @Override
                public String map(final Tuple2<LongWritable, DatabaseInputRecord> tuple) throws Exception {
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
        .writeAsText("hdfs:///" + parameterTool.get("hdfs.folder") + "/result.txt", WriteMode.OVERWRITE)
        .name("hdfs");

        // Setup Hadoop’s TextOutputFormat
        // HadoopOutputFormat<Text, LongWritable> hadoopOutputFormat =
        // new HadoopOutputFormat<Text, LongWritable>(
        // new TextOutputFormat<Text, LongWritable>(), new JobConf());
        // hadoopOutputFormat.getJobConf().set("mapred.textoutputformat.separator", " ");

        // FileOutputFormat.setOutputPath(hadoopOutputFormat.getJobConf(), new Path(outputPath));

        env.execute("Fetch data from database and store on HDFS");
    }
}
