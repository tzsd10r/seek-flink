/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.oclc.seek.flink.function.CountRecords;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.record.BaseObjectRowMapper;
import org.oclc.seek.flink.record.EntryFind;
import org.oclc.seek.flink.record.EntryFindRowMapper;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import scala.collection.mutable.StringBuilder;

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
public class QueryStreamToDbToKafkaJob extends JobGeneric {
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

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        final String prefix = parameterTool.getRequired("db.table");
        // // final String driver = parameterTool.getRequired("db.driver");
        // final String url = parameterTool.getRequired("db.url");
        // final String user = parameterTool.getRequired("db.user");
        // final String password = parameterTool.getRequired("db.password");

        /*
         * Query Generator stream
         */
        DataStream<String> queries = env
            .addSource(new QueryGeneratorStream())
            .map(new CountRecords<String>())
            .name("generator of queries")
            /*
             * Enforces the even distribution over all parallel instances of the following task
             */
            .rebalance();

        DataStream<List<EntryFind>> records = queries.map(new DatabaseRecordsFetcher()).name("get db records");

        DataStream<String> jsonRecords = records.flatMap(new FlatMapFunction<List<EntryFind>, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void flatMap(final List<EntryFind> records, final Collector<String> collector) throws Exception {
                for (EntryFind entryFind : records) {
                    collector.collect(entryFind.toJson());
                }
            }
        }).name("transform db records into json");

        // jsonRecords.addSink(
        // new KafkaSinkBuilder().build(
        // parameterTool.get("kafka.sink.topic." + prefix),
        // parameterTool.getProperties()))
        // .name("kafka");

        env.execute("Receives SQL queries... executes them and then writes to Kafka stage");
    }

    /**
     *
     */
    public class DatabaseRecordsFetcher extends RichMapFunction<String, List<EntryFind>> {
        private static final long serialVersionUID = 1L;
        private LongCounter recordCount = new LongCounter();
        private transient JdbcTemplate jdbcTemplate;
        private transient JdbcCursorItemReader<EntryFind> reader;
        private DataSource datasource;
        private EntryFind entryFind;

        @Override
        public void open(final Configuration configuration) throws Exception {
            super.open(configuration);

            ParameterTool parameters =
                (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

            String url = parameters.getRequired("db.url");
            String user = parameters.getRequired("db.user");
            String password = parameters.getRequired("db.password");

            getRuntimeContext().addAccumulator("recordCount", recordCount);
            datasource = new DriverManagerDataSource(url, user, password);
            jdbcTemplate = new JdbcTemplate(datasource);
            // jdbcTemplate.setFetchSize(2000);
            // jdbcTemplate = new StreamingResultSetEnabledJdbcTemplate(datasource);

            reader = new JdbcCursorItemReader<EntryFind>();
            reader.setDataSource(datasource);
            reader.setRowMapper(new EntryFindRowMapper());
        }

        @Override
        public List<EntryFind> map(final String query) throws Exception {
            reader.setSql(query);
            reader.open(new ExecutionContext());
            long counter = 0;
            // List<EntryFind> list = new ArrayList<EntryFind>();
            while (true) {
                entryFind = reader.read();
                if (entryFind == null) {
                    break;
                }
                // list.add(entryFind);
                counter++;
            }

            reader.close();
            recordCount.add(counter);;
            return new ArrayList<EntryFind>();
            // recordCount.add(list.size());
            // return list;
        }

    }

    /**
     * @param <T>
     */
    public class GenericRowCallbackHandler<T> implements RowCallbackHandler {
        private List<T> list = new ArrayList<T>();
        private BaseObjectRowMapper<T> rowMapper;

        /**
         * @param mapper
         */
        public GenericRowCallbackHandler(final BaseObjectRowMapper<T> rowMapper) {
            this.rowMapper = rowMapper;
        }

        @Override
        public void processRow(final ResultSet rs) throws SQLException {
            list.add(rowMapper.mapRow(rs));
        }

        /**
         * @return
         */
        public List<T> getList() {
            return list;
        }

        /**
         * @return
         */
        public int getSize() {
            return list.size();
        }
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
            StringBuilder value;
            for (String h : hex) {
                for (String e : hex) {
                    for (String x : hex) {
                        // for (String a : hex) {
                        value = new StringBuilder();
                        value.append(h);
                        value.append(e);
                        value.append(x);
                        // value.append(a);
                        ctx.collect("SELECT * FROM entry_find WHERE id LIKE '" + value + "%'");
                        // }
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
        QueryStreamToDbToKafkaJob job = new QueryStreamToDbToKafkaJob();
        job.init();
        job.execute(env);
    }

}
