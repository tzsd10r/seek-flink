/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
import org.springframework.jdbc.core.StatementCallback;

/**
 *
 */
public class DBFetcherCallBack2 extends RichFlatMapFunction<String, EntryFind> {
    private static final long serialVersionUID = 1L;
    private LongCounter recordCount = new LongCounter();
    private BaseObjectRowMapper<EntryFind> rowMapper;
    private int counter;
    /**
     * Note that anything else but Integer.MIN_VALUE has no effect on the MySQL driver
     */
    private int FETCH_SIZE = Integer.MIN_VALUE;
    private JdbcTemplate jdbcTemplate;
    
    /**
     * Concise description of what this class represents.
     */
    public static final String DESCRIPTION = "Fetcher records from database using callback 2";

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

        StatementCallback<Integer> callback = new StatementCallback<Integer>() {
            @Override
            public Integer doInStatement(Statement st) throws SQLException, DataAccessException {
                st = st.getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                st.setFetchSize(FETCH_SIZE);

                ResultSet rs = st.executeQuery(query);

                while (rs.next()) {
                    collector.collect(rowMapper.mapRow(rs));
                    counter++;
                }

                rs.close();
                st.close();

                return counter;
            }
        };

        Integer numberOfRecords = jdbcTemplate.execute(callback);
        recordCount.add(numberOfRecords);
    }
}
