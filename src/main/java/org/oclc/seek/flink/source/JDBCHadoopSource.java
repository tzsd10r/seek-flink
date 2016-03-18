/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.source;

import java.io.IOException;

import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.oclc.seek.flink.record.DbInputRecord;

/**
 *
 */
public class JDBCHadoopSource {
    /**
     * Concise description of what this class represents.
     */
    public static final String DESCRIPTION = "Extracts records from database using Map/Reduce.";

    /**
     * @param dbInputRecord
     * @param clazz
     * @param parameterTool
     * @return an instance of {@link HadoopInputFormat}
     * @throws IOException
     */
    public HadoopInputFormat<LongWritable, DbInputRecord> build(final ParameterTool parameterTool) throws IOException {
        JobConf conf = new JobConf();

        DBConfiguration.configureDB(conf,
            parameterTool.getRequired("db.driver"),
            parameterTool.getRequired("db.url"),
            parameterTool.getRequired("db.user"),
            parameterTool.getRequired("db.password"));

        DBInputFormat.setInput(conf,
            DbInputRecord.class,
            parameterTool.getRequired("db.table"),
            null,
            null,
            new String[] {
            parameterTool.getRequired("db.fields")
        });

        HadoopInputFormat<LongWritable, DbInputRecord> hadoopInputFormat =
            new HadoopInputFormat<LongWritable, DbInputRecord>(
                new DBInputFormat(), LongWritable.class, DbInputRecord.class, conf);

        // conf.setStrings("mapred.jdbc.input.count.query", "select count(*) from entry_find");
        // conf.setStrings("mapreduce.jdbc.input.count.query", "select count(*) from entry_find");
        conf.setNumTasksToExecutePerJvm(1);

        conf.setNumMapTasks(parameterTool.getInt("map.tasks", 10));

        // conf.writeXml(System.out);

        return hadoopInputFormat;
    }
}
