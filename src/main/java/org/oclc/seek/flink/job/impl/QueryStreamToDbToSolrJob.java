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
import java.util.Map;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.com.google.common.collect.ImmutableMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.oclc.seek.flink.document.KbwcEntryDocument;
import org.oclc.seek.flink.function.CountRecords;
import org.oclc.seek.flink.function.DBFetcherResultSetExtractor;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.record.BaseObjectRowMapper;
import org.oclc.seek.flink.record.EntryFind;
import org.oclc.seek.flink.record.EntryFindRowMapper;
import org.oclc.seek.flink.sink.SolrSink;
import org.oclc.seek.flink.sink.SolrSinkBuilder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import scala.collection.mutable.StringBuilder;

import com.google.gson.Gson;

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
public class QueryStreamToDbToSolrJob extends JobGeneric {
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

        String zkHosts = parameterTool.getRequired(SolrSink.ZKHOSTS);
        String collection = parameterTool.getRequired(SolrSink.COLLECTION);
        Map<String, String> configMap = ImmutableMap
            .of(SolrSink.ZKHOSTS, zkHosts, SolrSink.COLLECTION, collection);

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

        DataStream<EntryFind> records =
            queries.flatMap(new DBFetcherResultSetExtractor())
            .name("get db records using result set extractor");

        DataStream<KbwcEntryDocument> documents =
            records.flatMap(new FlatMapFunction<EntryFind, KbwcEntryDocument>() {
                private static final long serialVersionUID = 1L;

                @Override
                public void flatMap(final EntryFind record, final Collector<KbwcEntryDocument> collector)
                    throws Exception {
                    // JsonSlurper slurper = new JsonSlurper().setType( JsonParserType.INDEX_OVERLAY
                    // ).slurper.parseText(jsonData)
                    collector.collect(new Gson().fromJson(record.toJson(), KbwcEntryDocument.class));
                }
            }).name("transform db records into json");

        documents.addSink(new SolrSinkBuilder<KbwcEntryDocument>().build(configMap))
        .name("Index to solr sink");;

        env.execute("Receives SQL queries... executes them and then writes to Solr");
    }

    /**
     *
     */
    public class DatabaseRecordsFetcherJdbcTemplate extends RichFlatMapFunction<String, EntryFind> {
        private static final long serialVersionUID = 1L;
        private LongCounter recordCount = new LongCounter();
        private transient JdbcTemplate jdbcTemplate;
        private BaseObjectRowMapper<EntryFind> rowMapper;
        private long counter;

        @Override
        public void open(final Configuration configuration) throws Exception {
            super.open(configuration);

            ParameterTool parameters =
                (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

            String url = parameters.getRequired("db.url");
            String user = parameters.getRequired("db.user");
            String password = parameters.getRequired("db.password");

            getRuntimeContext().addAccumulator("recordCount", recordCount);
            jdbcTemplate = new JdbcTemplate(new DriverManagerDataSource(url, user, password));
            rowMapper = new EntryFindRowMapper();
        }

        @Override
        public void flatMap(final String query, final Collector<EntryFind> collector) throws Exception {
            counter = 0;
            jdbcTemplate.query(query, new RowCallbackHandler() {
                @Override
                public void processRow(final ResultSet rs) throws SQLException {
                    collector.collect(rowMapper.mapRow(rs));
                    counter++;
                }
            });

            recordCount.add(counter);;
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
        System.setProperty("environment", "test");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        QueryStreamToDbToSolrJob job = new QueryStreamToDbToSolrJob();
        job.init();
        job.execute(env);
    }

}
