/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.stream.job;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.oclc.seek.flink.job.JobContract;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.job.runner.JobRunner;
import org.oclc.seek.flink.stream.sink.HdfsSink;

/**
 *
 */
public class QueryGeneratorToDbToHdfsJob extends JobGeneric implements JobContract {
    private Properties props = new Properties();

    @Override
    public void init() {
        props.put("group.id", "seek-kafka");
        props.put("zookeeper.connect",
            "ilabhddb03dxdu.dev.oclc.org:9011,ilabhddb04dxdu.dev.oclc.org:9011,ilabhddb05dxdu.dev.oclc.org:9011");
        props.put("kafka.topic", "test-events");
        props.put("bootstrap.servers", "ilabhddb03dxdu:9077,ilabhddb04dxdu:9077,ilabhddb05dxdu:9077");
        props.put("hdfs.folder", "/user/seabrae/flink");
        props.put("hdfs.host", "hdfs://ilabhddb02dxdu.dev.oclc.org:9008");

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

        // add a simple source which is writing some strings
        env.addSource(new SimpleStringGenerator())
        .rebalance()
        .map(new MapFunction<String, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String map(final String value) throws Exception {
                new Thread(
                    new Runnable() {
                        @Override
                        public void run() {
                            new JobRunner().run("dbimportentryfindjob", value);
                        }
                    }, "DBImportEntryFindJob").start();;

                    return "Query: " + value + " is complete!";
            }
        }).name("entry-find-queries")
        .addSink(new HdfsSink().build(parameterTool.get("hdfs.folder")))
        .name("hdfs");

        env.execute("Generates Queries... executes them and writes results to HDFS");
    }

    /**
     *
     */
    public static class SimpleStringGenerator implements SourceFunction<String> {
        private static final long serialVersionUID = 2174904787118597072L;
        boolean running = true;
        long i = 1;

        @Override
        public void run(final SourceContext<String> ctx) throws Exception {
            while (running && i <= 1) {
                ctx.collect("select owner_institution, collection_ui from entry_find limit " + i++);
                Thread.sleep(10);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
