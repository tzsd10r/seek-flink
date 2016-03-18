/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import scala.collection.mutable.StringBuilder;

/**
 *
 */
public class QueryGeneratorSource implements SourceFunction<String> {
    /**
     * Concise description of what this class represents.
     */
    public static final String DESCRIPTION = "Generator of SQL queries.";
    private static final long serialVersionUID = 1L;
    boolean running = true;
    long i = 1;

    static final String[] hex = {
        "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"
    };

    @Override
    public void run(final SourceContext<String> ctx) throws Exception {
        StringBuilder value;
        for (String h : hex) {
            for (String e : hex) {
                // for (String x : hex) {
                // for (String a : hex) {
                value = new StringBuilder();
                value.append(h);
                value.append(e);
                // value.append(x);
                // value.append(a);
                ctx.collect("SELECT * FROM entry_find WHERE id LIKE '" + value + "%'");
                // System.out.println("SELECT * FROM entry_find WHERE id LIKE '" + value + "%'");
                Thread.sleep(100);
            }
            // }
            // }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
