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

import scala.collection.mutable.StringBuilder;

/**
 *
 */
public class QueryLikeSource extends RichSourceFunction<String> {
    private static final long serialVersionUID = 1L;
    /**
     * Concise description of what this class represents.
     */
    public static final String DESCRIPTION = "Generator of SQL queries";

    static final String[] hex = {
        "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"
    };
    
    private String table;
    
    @Override
    public void open(final Configuration configuration) throws Exception {
        super.open(configuration);

        ParameterTool parameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        table = parameters.getRequired("db.table");
    }

    @Override
    public void run(final SourceContext<String> ctx) throws Exception {
        StringBuilder value;
        for (String h : hex) {
            for (String e : hex) {
                //for (String x : hex) {
                value = new StringBuilder();
                value.append(h);
                value.append(e);
                //value.append(x);
                ctx.collect("SELECT * FROM " + table + " WHERE id LIKE '" + value + "%'");
                // System.out.println("SELECT * FROM entry_find WHERE id LIKE '" + value + "%'");
                //}
            }
        }
    }
    
    @Override
    public void cancel() {
        // running = false;
    }
}
