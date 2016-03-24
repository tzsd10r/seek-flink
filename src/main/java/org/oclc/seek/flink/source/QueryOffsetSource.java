/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.source;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 *
 */
public class QueryOffsetSource extends RichSourceFunction<String> {
    private static final long serialVersionUID = 1L;
    /**
     * Concise description of what this class represents.
     */
    public static final String DESCRIPTION = "Builds query with offset values based on parallelism";
    private transient JdbcTemplate jdbcTemplate;
    private String table;
    private int split;

    public QueryOffsetSource(int parallelism) {
        this.split = parallelism;
    }

    @Override
    public void open(final Configuration configuration) throws Exception {
        super.open(configuration);

        ParameterTool parameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

        String url = parameters.getRequired("db.url");
        String user = parameters.getRequired("db.user");
        String password = parameters.getRequired("db.password");
        table = parameters.getRequired("db.table");

        jdbcTemplate = new JdbcTemplate(new DriverManagerDataSource(url, user, password));
    }

    @Override
    public void run(final SourceContext<String> ctx) throws Exception {
        int rows = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + table, Integer.class);

        int chunk = rows / split;

        /*
         * mod will always be less than the split
         */
        int mod = rows % split;
        String query;

        System.out.println("rows  : " + rows);
        System.out.println("chunk : " + chunk);
        System.out.println("mod   : " + mod);
        System.out.println("split : " + split);
        System.out.println("------------------------------------------------");

        int offset = 0;
        // System.out.println("i loop is            : " + 0);
        // System.out.println("offset calculated    : " + 0);
        for (int i = 0; i < split; i++) {
            if (i < mod) {
                // System.out.println("what is chunk (" + chunk + " + " + 1 + ")  : " + (chunk + 1));
                // System.out.println("what is offset (" + offset + " + " + i + "): " + (offset + i));
                query = "SELECT * FROM " + table + " LIMIT " + (chunk + 1) + " OFFSET " + (offset + i);
            } else {
                // System.out.println("what is chunk                             : " + chunk);
                // System.out.println("what is offset (" + offset + " + " + mod + "): " + (offset + mod));
                query = "SELECT * FROM " + table + " LIMIT " + chunk + " OFFSET " + (offset + mod);
            }

            System.out.println(i + " : " + query);
            ctx.collect(query);
            // System.out.println("------------------------------------------------");

            offset = (i + 1) * chunk;
            // System.out.println("i loop is (" + i + " + " + 1 + ")                             : " + (i + 1));
            // System.out.println("offset calculated ((" + i + " + " + 1 + " ) * " + chunk + ")  : " + offset);
        }
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        new QueryOffsetSource(20).run(null);
    }

    @Override
    public void cancel() {
    }
}
