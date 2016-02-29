/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.batch.source;

import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat.JDBCInputFormatBuilder;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * @param <TOUT>
 */
public class JDBCSource<TOUT extends Tuple> {
    /**
     * @param parms
     * @return an instance of {@link JDBCInputFormat}
     */
    public JDBCInputFormat<TOUT> build(final ParameterTool parms) {
        String driver = parms.get("driver");
        String url = parms.get("url");
        String user = parms.get("user");
        String password = parms.get("password");
        String fields = parms.get("fields");
        String table = parms.get("table");
        String conditions = parms.get("conditions");

        StringBuilder query = new StringBuilder(parms.get("query", ""));

        if (query.length() == 0) {
            System.out.println("Query was not provided... building query...");
            query.append("select");
            query.append(" ");
            query.append(fields);
            query.append(" ");
            query.append("from");
            query.append(" ");
            query.append(table);
            query.append(" ");
            query.append(conditions);
        }

        System.out.println(query);

        @SuppressWarnings("unchecked")
        JDBCInputFormat<TOUT> inputFormat = new JDBCInputFormatBuilder().setDrivername(driver).setDBUrl(url)
        .setQuery(query.toString()).setUsername(user).setPassword(password).finish();

        return inputFormat;
    }
}
