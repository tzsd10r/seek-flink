/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.impl;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.source.QueryOffsetSource;

/**
 *
 */
public class QueryStreamJob extends JobGeneric {
    private static final long serialVersionUID = 1L;

    @Override
    public void init() {
        super.init();
    }

    /**
     * @throws Exception
     */
    @Override
    public void execute(final StreamExecutionEnvironment env) throws Exception {
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);
        
        System.out.println("parallelism :" +  env.getParallelism());

        //DataStream<String> queries = env.addSource(new QueryLikeSource()).name(QueryLikeSource.DESCRIPTION);
        DataStream<String> queries = env.addSource(new QueryOffsetSource(env.getParallelism()))
            .name(QueryOffsetSource.DESCRIPTION);

        queries.print();

        env.execute("Receives SQL queries... and prints");
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        System.setProperty("environment", "test");
        System.setProperty("json.text.parser", "gson");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        QueryStreamJob job = new QueryStreamJob();
        job.init();
        job.execute(env);
    }
}
