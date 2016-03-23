/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.mapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.oclc.seek.flink.record.BaseObjectRowMapper;
import org.oclc.seek.flink.record.EntryFind;
import org.oclc.seek.flink.record.EntryFindRowMapper;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 *
 */
public class DBFetcherCallBack extends RichFlatMapFunction<String, EntryFind> {
    private static final long serialVersionUID = 1L;
    /**
     * Concise description of what this class represents.
     */
    public static final String DESCRIPTION = "Fetcher records from database using callback.";
    private LongCounter recordCount = new LongCounter();
    private transient JdbcTemplate jdbcTemplate;
    private BaseObjectRowMapper<EntryFind> rowMapper;
    private int counter;

    /*
     * Note that anything else but Integer.MIN_VALUE has no effect on the MySQL driver
     */
    private static int FETCH_SIZE = Integer.MIN_VALUE;

    @Override
    public void open(final Configuration configuration) throws Exception {
        super.open(configuration);

        ParameterTool parameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

        // String driver = parameters.getRequired("db.driver");
        String url = parameters.getRequired("db.url");
        String user = parameters.getRequired("db.user");
        String password = parameters.getRequired("db.password");

        /*
         * If instantiated BasicDataSource... the sessions are kept alive and don't go away.
         * Need to figure what to do in order to have sessions closed.
         */

        // BasicDataSource datasource = new BasicDataSource();
        // datasource.setDriverClassName(driver);
        // datasource.setUsername(user);
        // datasource.setPassword(password);
        // datasource.setUrl(url);
        // datasource.setValidationQuery("SELECT 1");
        // datasource.setTestOnBorrow(true);
        // jdbcTemplate = new JdbcTemplate(datasource);

        getRuntimeContext().addAccumulator("recordCount", recordCount);

        jdbcTemplate = new JdbcTemplate(new DriverManagerDataSource(url, user, password));
        rowMapper = new EntryFindRowMapper();
    }

    @Override
    public void flatMap(final String query, final Collector<EntryFind> collector) throws Exception {
        counter = 0;

        PreparedStatementCallback<Integer> callback = new PreparedStatementCallback<Integer>() {
            @Override
            public Integer doInPreparedStatement(final PreparedStatement pstmt) throws SQLException,
                DataAccessException {

                ResultSet rs = pstmt.executeQuery();
                while (rs.next()) {
                    collector.collect(rowMapper.mapRow(rs));
                    counter++;
                }
                rs.close();
                return counter;
            }
        };

        PreparedStatementCreator creator = new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(final Connection conn) throws SQLException {
                /*
                 * These values make a big difference.
                 * Most default to ResultSet.TYPE_SCROLL_INSENSITIVE and ResultSet.CONCUR_READ_ONLY);
                 * The culprit here is the TYPE_SCROLL_INSENSITIVE which is causing a memory leak when fetching
                 * large amounts of data.
                 * When replaced with TYPE_FORWARD_ONLY, the resources are better managed and... no more memory
                 * leaks.
                 * Just as important is to set the fetchSize.
                 */
                PreparedStatement ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
                conn.setAutoCommit(false);
                ps.setFetchSize(FETCH_SIZE);
                /*
                 * Query time out in seconds (total of 100 minutes)
                 */
                ps.setQueryTimeout(60 * 100);
                return ps;
            }
        };

        Integer numberOfRecords = jdbcTemplate.execute(creator, callback);
        recordCount.add(numberOfRecords);
    }
}
