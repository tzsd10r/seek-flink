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

/**
 *
 */
public class DBFetcherCallBack extends RichFlatMapFunction<String, EntryFind> {
    private static final long serialVersionUID = 1L;
    /**
     * Concise description of what this class represents.
     */
    public static final String DESCRIPTION = "Fetcher records from database using callback";
    private LongCounter recordCount = new LongCounter();
    private transient JdbcTemplate jdbcTemplate;
    private BaseObjectRowMapper<EntryFind> rowMapper;
    private int counter;
    /**
     * Note that anything else but Integer.MIN_VALUE has no effect on the MySQL driver
     */
    private static int FETCH_SIZE = Integer.MIN_VALUE;

    @Override
    public void open(final Configuration configuration) throws Exception {
        super.open(configuration);

        getRuntimeContext().addAccumulator("recordCount", recordCount);
        rowMapper = new EntryFindRowMapper();

      //jdbcTemplate = DBFetcherUtility.createPoolableJdbcTemplate((ParameterTool)getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        jdbcTemplate = DBFetcherUtility.createJdbcTemplate((ParameterTool)getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
    }

    @Override
    public void flatMap(final String query, final Collector<EntryFind> collector) throws Exception {
        counter = 0;

        PreparedStatementCallback<Integer> callback = new PreparedStatementCallback<Integer>() {
            @Override
            public Integer doInPreparedStatement(final PreparedStatement ps) throws SQLException,
                DataAccessException {

                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    collector.collect(rowMapper.mapRow(rs));
                    counter++;
                }

                rs.close();
                /*
                 * Do I really need to do ps.close()?????
                 * I thought I needed this because the connections were staying in the database in 'sleep' mode... but
                 * then found out that the wait_timeout variable is what really determines how long a connection will
                 * stay in 'sleep' mode in the database.
                 */
                ps.close();
                
                return counter;
            }
        };

        PreparedStatementCreator creator = new PreparedStatementCreator() {
            @Override
            public PreparedStatement createPreparedStatement(final Connection conn) throws SQLException {
                /*
                 * These values make a big difference.
                 * Most default to ResultSet.TYPE_SCROLL_INSENSITIVE and ResultSet.CONCUR_READ_ONLY);
                 * 
                 * The culprit here is the TYPE_SCROLL_INSENSITIVE which is causing a memory leak when fetching large
                 * amounts of data.
                 * 
                 * When replaced with TYPE_FORWARD_ONLY, the resources are better managed and... no more memory leaks.
                 * Just as important... is to set the fetchSize.
                 */
                PreparedStatement ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
                conn.setAutoCommit(false);

                ps.setFetchSize(FETCH_SIZE);

                /*
                 * Query time out in seconds (total of 120 minutes)
                 */
                ps.setQueryTimeout(7200);
                
                return ps;
            }
        };
        
//        System.out.println("getMaxWaitMillis           : " + ((BasicDataSource)jdbcTemplate.getDataSource()).getMaxWaitMillis());
//        System.out.println("getMaxIdle                 : " + ((BasicDataSource)jdbcTemplate.getDataSource()).getMaxIdle());
//        System.out.println("getMaxConnLifetimeMillis   : " + ((BasicDataSource)jdbcTemplate.getDataSource()).getMaxConnLifetimeMillis());
//        System.out.println("getEnableAutoCommitOnReturn: " + ((BasicDataSource)jdbcTemplate.getDataSource()).getEnableAutoCommitOnReturn());
//        System.out.println("getDefaultQueryTimeout     : " + ((BasicDataSource)jdbcTemplate.getDataSource()).getDefaultQueryTimeout());
//        System.out.println("getDefaultAutoCommit       : " + ((BasicDataSource)jdbcTemplate.getDataSource()).getDefaultAutoCommit());

        Integer numberOfRecords = jdbcTemplate.execute(creator, callback);
        recordCount.add(numberOfRecords);
    }
}
