/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.impl;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.hadoop.io.LongWritable;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.mapper.ObjectToJsonTransformer;
import org.oclc.seek.flink.record.DbInputRecord;
import org.oclc.seek.flink.source.JDBCHadoopSource;

/**
 * Uses the HadoopInputFormat type to define the source for the database.
 * Under the hood... it uses map/reduce technology to fetch the rows from the database.
 * 
 * Additionally, the Flink environment initialized here uses the Batch API.
 * 
 * NOTE: NOT WORKING PROPERLY!!! FOR SOME REASON... DB CONNECTIONS ARE MAXING OUT.
 */
public class DBHadoopBatchToHdfsJob extends JobGeneric {
    private static final long serialVersionUID = 1L;

    @Override
    public void init() {
        super.init();
    }

    @Override
    public void execute(final ExecutionEnvironment env) throws Exception {
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        DataSet<Tuple2<LongWritable, DbInputRecord>> records = env.createInput(
            new JDBCHadoopSource(parameterTool).get()).name(JDBCHadoopSource.DESCRIPTION);

        DataSet<String> jsonRecords = records.map(new ObjectToJsonTransformer<Tuple2<LongWritable, DbInputRecord>>())
            .name(ObjectToJsonTransformer.DESCRIPTION);

        String path = parameterTool.get("fs.sink.dir." + parameterTool.get("db.table"));
        jsonRecords.writeAsText(path + "/entry-find.txt", WriteMode.OVERWRITE).name("filesystem sink");

        env.execute("Fetch data from database and store on the Filesystem");
    }
}
