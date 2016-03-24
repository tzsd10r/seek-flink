/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.mapper;

import javax.sql.DataSource;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.oclc.seek.flink.record.EntryFind;
import org.oclc.seek.flink.record.EntryFindRowMapper;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 *
 */
public class DBFetcherItemReader extends RichFlatMapFunction<String, EntryFind> {
    private static final long serialVersionUID = 1L;
    private LongCounter recordCount = new LongCounter();
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

        reader = new JdbcCursorItemReader<EntryFind>();
        reader.setDataSource(datasource);
        /*
         * Don't ever use fetchSize() method with JdbcCursorItemReader.
         * It throws a com.mysql.jdbc.OperationNotSupportedException: Operation not supported for streaming
         * result sets
         */
        // reader.setFetchSize(FETCH_SIZE);
        reader.setRowMapper(new EntryFindRowMapper());
        reader.setQueryTimeout(7200);
    }

    @Override
    public void flatMap(final String query, final Collector<EntryFind> collector) throws Exception {
        reader.setSql(query);
        reader.open(new ExecutionContext());
        long counter = 0;

        while (true) {
            entryFind = reader.read();
            if (entryFind == null) {
                break;
            }
            collector.collect(entryFind);
            counter++;
        }

        reader.close();

        recordCount.add(counter);;
    }
}
