/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.impl;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.LongWritable;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.record.DbInputRecord;
import org.oclc.seek.flink.source.JDBCHadoopSource;

/**
 *
 */
public class DbToHdfsJob extends JobGeneric {
    private static final long serialVersionUID = 1L;

    @Override
    public void init() {
        super.init();
    }

    @Override
    public void execute(final StreamExecutionEnvironment env) throws Exception {
        // create a checkpoint every 1000 ms
        // env.enableCheckpointing(1000);

        // set the timeout low to minimize latency
        // env.setBufferTimeout(10);

        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        // JobConf conf = new JobConf();
        //
        // DBConfiguration.configureDB(conf,
        // parameterTool.getRequired("db.driver"),
        // parameterTool.getRequired("db.url"),
        // parameterTool.getRequired("db.user"),
        // parameterTool.getRequired("db.password"));
        //
        // DBInputFormat.setInput(conf,
        // DbInputRecord.class,
        // parameterTool.getRequired("db.table"),
        // null,
        // null,
        // new String[] {
        // parameterTool.getRequired("db.fields")
        // });
        //
        // // DBInputFormat.setInput(conf,
        // // DbInputRecord.class,
        // // "select * from " + table,
        // // "select count(*) from" + table
        // // );
        //
        // HadoopInputFormat<LongWritable, DbInputRecord> hadoopInputFormat =
        // new HadoopInputFormat<LongWritable, DbInputRecord>(
        // new DBInputFormat(), LongWritable.class, DbInputRecord.class, conf);
        //
        // // conf.setStrings("mapred.jdbc.input.count.query", "select count(*) from entry_find");
        // // conf.setStrings("mapreduce.jdbc.input.count.query", "select count(*) from entry_find");
        // // conf.setNumTasksToExecutePerJvm(1);
        //
        // conf.setNumMapTasks(parameterTool.getInt("map.tasks", 6));
        // DataStream<Tuple2<LongWritable, DbInputRecord>> rawRecords =
        // env.createInput(hadoopInputFormat).name("db source");

        DataStream<Tuple2<LongWritable, DbInputRecord>> rawRecords =
            env.createInput(new JDBCHadoopSource().build(parameterTool)).name("db source");

        // DataStream<String> jsonRecords =
        DataStream<Tuple3<String, Long, Long>> jsonRecords =
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
            })
            .keyBy(0)
            .countWindow(10)
            .apply(new WindowFunction<String, Tuple3<String, Long, Long>, Tuple, GlobalWindow>() {
                private static final long serialVersionUID = 1L;

                @Override
                public void apply(final Tuple arg0, final GlobalWindow window,
                    final Iterable<String> values,
                    final Collector<Tuple3<String, Long, Long>> collector) throws Exception {
                    Long elapsedMillis = window.maxTimestamp();

                    collector.collect(new Tuple3<String, Long, Long>("Words: ", elapsedMillis,
                        new Long(Iterables.size(values))));
                }
            });

        // .name("convert db record into json");

        // DataStreamSink<String> filesystem =
        // jsonRecords.addSink(
        // new HdfsSinkBuilder().build(parameterTool.get(parameterTool.getRequired("db.table") + ".fs.sink.dir")))
        // .name("put json records on filesystem");

        env.execute("Queries the DB and drops results onto Filesystem");
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        System.setProperty("environment", "test");
        System.setProperty("map.tasks", "6");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DbToHdfsJob job = new DbToHdfsJob();
        job.init();
        job.execute(env);
    }
}
