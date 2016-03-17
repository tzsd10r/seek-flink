/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.function;

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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 *
 */
public class DBFetcherRowMapper extends RichFlatMapFunction<String, EntryFind> {
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
