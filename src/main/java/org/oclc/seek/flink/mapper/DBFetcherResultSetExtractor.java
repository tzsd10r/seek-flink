/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.mapper;

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
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;

/**
 *
 */
public class DBFetcherResultSetExtractor extends RichFlatMapFunction<String, EntryFind> {
    private static final long serialVersionUID = 1L;
    /**
     * Concise description of what this class represents.
     */
    public static final String DESCRIPTION = "Fetcher records from database using result set extractor";
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
        
        jdbcTemplate = DBFetcherUtility.createJdbcTemplate((ParameterTool)getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        //jdbcTemplate = DBFetcherUtility.createPoolableJdbcTemplate((ParameterTool)getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
    }

    @Override
    public void flatMap(final String query, final Collector<EntryFind> collector) throws Exception {
        counter = 0;

        Integer numberOfRecords = jdbcTemplate.query(query, new PreparedStatementSetter() {
            @Override
            public void setValues(final PreparedStatement ps) throws SQLException {
                ps.getConnection().setAutoCommit(false);
                ps.getConnection().prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                ps.setFetchSize(FETCH_SIZE);
                ps.setQueryTimeout(7200);
            }
        }, new ResultSetExtractor<Integer>() {
            @Override
            public Integer extractData(final ResultSet rs) throws SQLException, DataAccessException {
                while (rs.next()) {
                    collector.collect(rowMapper.mapRow(rs));
                    counter++;
                }
                rs.close();
                
                return counter;
            }
        });

        recordCount.add(numberOfRecords);
    }
}
