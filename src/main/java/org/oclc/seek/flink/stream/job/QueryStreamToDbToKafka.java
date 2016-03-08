/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.stream.job;

import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.oclc.seek.flink.job.JobContract;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.stream.sink.KafkaSinkBuilder;

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
public class QueryStreamToDbToKafka extends JobGeneric implements JobContract, Serializable {
    private static final long serialVersionUID = 1L;
    private Properties props = new Properties();

    // private JdbcTemplate jdbcTemplate;

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

    /**
     * @throws Exception
     */
    @Override
    public void execute(final StreamExecutionEnvironment env) throws Exception {
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.getConfig().disableSysoutLogging();
        // use system default value
        env.getConfig().setNumberOfExecutionRetries(5);
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.enableCheckpointing(5000); // create a checkpoint every 5 secodns

        final String prefix = parameterTool.getRequired("db.table");
        final String driver = parameterTool.getRequired("db.driver");
        final String url = parameterTool.getRequired("db.url");
        final String user = parameterTool.getRequired("db.user");
        final String password = parameterTool.getRequired("db.password");

        // jdbcTemplate = new JdbcTemplate(dataSource);

        /*
         * Query Generator stream
         */
        DataStream<String> queries = env
            .addSource(new QueryGeneratorStream())
            .name("generator of queries")
            .rebalance();

        DataStream<String> enrichedJsonRecords = queries.map(new RichMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            private LongCounter recordCount = new LongCounter();

            @Override
            public void open(final Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("recordCount", recordCount);
            }

            @Override
            public String map(final String query) throws Exception {
                // List<DbInputRecord> record = jdbcTemplate.query(
                // "select first_name, last_name from t_actor",
                // new RowMapper<Actor>() {
                // public Actor mapRow(ResultSet rs, int rowNum) throws SQLException {
                // Actor actor = new Actor();
                // actor.setFirstName(rs.getString("first_name"));
                // actor.setLastName(rs.getString("last_name"));
                // return actor;
                // }
                // });

                recordCount.add(1L);
                return prefix + ":{" + query + "}";
            }
        }).name("add root element to json record");

        DataStreamSink<String> kafka = enrichedJsonRecords.addSink(
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
        private static final long serialVersionUID = 2174904787118597072L;
        boolean running = true;
        long i = 1;

        @Override
        public void run(final SourceContext<String> ctx) throws Exception {
            while (running && i <= 100) {
                ctx.collect("select * from entry_find where ");
                Thread.sleep(10);
            }
        }

        // @Override
        // public void run(final SourceContext<DbInputRecord> ctx) throws Exception {
        // while (running && i <= 100) {
        // ctx.collect(new DbInputRecordBuilder().ownerInstitution(91475L + i++)
        // .collectionUid("wiley.interScience")
        // .build());
        // Thread.sleep(10);
        // }
        // }

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
