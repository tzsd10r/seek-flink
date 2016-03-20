/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.impl;

import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.mapper.CountRecords;
import org.oclc.seek.flink.source.QueryGeneratorSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;

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
public class QueryStreamJob extends JobGeneric {
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

        /*
         * Query Generator stream
         */
        DataStream<String> queries = env
            .addSource(new QueryGeneratorSource()).map(new CountRecords<String>())
            .name(QueryGeneratorSource.DESCRIPTION)
            .rebalance();

        env.execute("Receives SQL queries... executes them and then writes to Kafka stage");
    }

    /**
     * A {@link JdbcTemplate} which will make it possible to mimic streaming Resultset's by allowing negative fetch
     * sizes
     * to be set on the {@link Statement}.
     *
     * @author reik.schatz
     */
    public class StreamingResultSetEnabledJdbcTemplate extends JdbcTemplate {
        public StreamingResultSetEnabledJdbcTemplate(final DataSource dataSource) {
            super(dataSource);
        }

        public StreamingResultSetEnabledJdbcTemplate(final DataSource dataSource, final boolean lazyInit) {
            super(dataSource, lazyInit);
        }

        /**
         * Prepare the given JDBC Statement (or PreparedStatement or CallableStatement),
         * applying statement settings such as fetch size, max rows, and query timeout.
         * Unlike in {@link JdbcTemplate} you can also specify a negative fetch size.
         *
         * @param stmt the JDBC Statement to prepare
         * @throws java.sql.SQLException if thrown by JDBC API
         * @see #setFetchSize
         * @see #setMaxRows
         * @see #setQueryTimeout
         * @see org.springframework.jdbc.datasource.DataSourceUtils#applyTransactionTimeout
         */
        @Override
        protected void applyStatementSettings(final Statement stmt) throws SQLException {
            int fetchSize = getFetchSize();
            stmt.setFetchSize(fetchSize);

            int maxRows = getMaxRows();
            if (maxRows > 0) {
                stmt.setMaxRows(maxRows);
            }

            DataSourceUtils.applyTimeout(stmt, getDataSource(), getQueryTimeout());
        }
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        System.setProperty("environment", "test");
        System.setProperty("map.tasks", "10");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        QueryStreamJob job = new QueryStreamJob();
        job.init();
        job.execute(env);
    }

    // System.out.println(query);
    // GenericRowCallbackHandler<EntryFind> handler =
    // new GenericRowCallbackHandler<EntryFind>(new EntryFindRowMapper());
    // jdbcTemplate.query(query, handler);
    // recordCount.add(handler.getSize());
    // return handler.getList();

    // List<EntryFind> records =
    // jdbcTemplate.query(query, new RowMapper<EntryFind>() {
    // @Override
    // public EntryFind mapRow(final ResultSet rs, final int i) throws SQLException {
    // return new EntryFindRowMapper().mapRow(rs);
    // }
    // });
    //
    // recordCount.add(records.size());
    // return records;

    // PreparedStatementCallback<List<EntryFind>> callback = new
    // PreparedStatementCallback<List<EntryFind>>() {
    // @Override
    // public List<EntryFind> doInPreparedStatement(final PreparedStatement pstmt) throws SQLException,
    // DataAccessException {
    // List<EntryFind> list = new ArrayList<EntryFind>();
    //
    // ResultSet rs = pstmt.executeQuery();
    // while (rs.next()) {
    // list.add(EntryFindRowMapper.mapRow(rs));
    // }
    // rs.close();
    // return list;
    // }
    // };
    //
    // PreparedStatementCreator creator = new PreparedStatementCreator() {
    // @Override
    // public PreparedStatement createPreparedStatement(final Connection conn) throws SQLException {
    // PreparedStatement pstmt =
    // conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    // pstmt.setFetchSize(Integer.MIN_VALUE);
    // return pstmt;
    // }
    // };
    //
    // List<EntryFind> records = jdbcTemplate.execute(creator, callback);
    // recordCount.add(records.size());
    // return records;

}
