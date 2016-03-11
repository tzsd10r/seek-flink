/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.impl;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.record.EntryFind;
import org.oclc.seek.flink.record.EntryFindMapper;
import org.oclc.seek.flink.sink.KafkaSinkBuilder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * Here, you can start creating your execution plan for Flink.
 * <p>
 * Start with getting some data from the environment, as follows:
 *
 * <pre>
 * env.readTextFile(textPath);
 * </pre>
 *
 * ...then, transform the resulting DataStream<T> using operations like the following:
 * <p>
 * .filter() <br>
 * .flatMap() <br>
 * .join() <br>
 * .group()
 * <p>
 * ...and many more.
 * <p>
 * Have a look at the programming guide and examples:
 * <p>
 * http://flink.apache.org/docs/latest/programming_guide.html<br>
 * http://flink.apache.org/docs/latest/examples.html <br>
 * <p>
 * Note that the Kafka source/sink is expecting the following parameters to be set <br>
 * - "bootstrap.servers" (comma separated list of kafka brokers) <br>
 * - "zookeeper.connect" (comma separated list of zookeeper servers) <br>
 * - "group.id" the id of the consumer group <br>
 * - "topic" the name of the topic to read data from.
 */
public class QueryStreamToDbToKafka extends JobGeneric implements Serializable {
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
        // create a checkpoint every 5 secodns
        // env.enableCheckpointing(5000);

        // defines how many times the job is restarted after a failure
        // env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 60000));

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);


        final String prefix = parameterTool.getRequired("db.table");
        // final String driver = parameterTool.getRequired("db.driver");
        final String url = parameterTool.getRequired("db.url");
        final String user = parameterTool.getRequired("db.user");
        final String password = parameterTool.getRequired("db.password");

        final DataSource datasource = new DriverManagerDataSource(url, user, password);
        final JdbcTemplate jdbcTemplate = new JdbcTemplate(datasource);

        /*
         * Query Generator stream
         */
        DataStream<String> queries = env
            .addSource(new QueryGeneratorStream())
            .name("generator of queries")
            .rebalance();

        DataStream<List<EntryFind>> records = queries.map(new RichMapFunction<String, List<EntryFind>>() {
            private static final long serialVersionUID = 1L;
            private LongCounter recordCount = new LongCounter();

            // private transient JdbcTemplate jdbcTemplate;

            @Override
            public void open(final Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("recordCount", recordCount);
                // jdbcTemplate = new JdbcTemplate(datasource);
            }

            @Override
            public List<EntryFind> map(final String query) throws Exception {
                List<EntryFind> records =
                    jdbcTemplate.query(query, new RowMapper<EntryFind>() {
                        @Override
                        public EntryFind mapRow(final ResultSet rs, final int i) throws SQLException {
                            return EntryFindMapper.mapRow(rs);
                        }
                    });

                recordCount.add(1L);
                return records;
            }
        }).name("add root element to json record");

        DataStream<String> jsonRecords = records.flatMap(new FlatMapFunction<List<EntryFind>, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void flatMap(final List<EntryFind> records, final Collector<String> collector) throws Exception {
                for (EntryFind entryFind : records) {
                    collector.collect(entryFind.toJson());
                }
            }
        });

        // DataStreamSink<String> kafka =
        jsonRecords.addSink(
            new KafkaSinkBuilder().build(
                parameterTool.get(prefix + ".kafka.stage.topic"),
                parameterTool.getProperties()))
                .name("kafka stage");

        env.execute("Read Events from Kafka Source and write to Kafka Stage");
    }

    /**
     *
     */
    public static class QueryGeneratorStream implements SourceFunction<String> {
        private static final long serialVersionUID = 1L;
        boolean running = true;
        long i = 1;

        static final String[] hex = {
            "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"
        };


        @Override
        public void run(final SourceContext<String> ctx) throws Exception {
            // while (running && i <= 1000) {
            // ctx.collect("select * from entry_find where ");
            // Thread.sleep(10);
            // }

            while (running && i <= 1000) {
                for (String s : hex) {
                    for (String h : hex) {
                        String value = s + h;
                        String query = "SELECT * FROM entry_find WHERE ID LIKE '" + value + "%'";
                        // System.out.println(query);
                        ctx.collect(query);
                    }
                }
            }

        }

        @Override
        public void cancel() {
            running = false;
        }
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
        QueryStreamToDbToKafka job = new QueryStreamToDbToKafka();
        job.init();
        job.execute(env);
    }

}
