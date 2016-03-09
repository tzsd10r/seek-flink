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
import org.apache.hadoop.conf.Configuration;
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
     * @param dbInputRecord
     * @param clazz
     * @return an instance of {@link HadoopInputFormat}
     * @throws IOException
     */
    public HadoopInputFormat<LongWritable, DbInputRecord> build(final DbInputRecord dbInputRecord,
        final Class<DbInputRecord> clazz) throws IOException {
        // Get job instance
        JobConf job = new JobConf();

        // Setup Hadoop DBInputFormat by creating a Flink Wrapper (HadoopInputFormat) with parameters that specify
        // the Hadoop InputFormat, the KEY and VALUE types, and the job
        HadoopInputFormat<LongWritable, DbInputRecord> hadoopInputFormat =
            new HadoopInputFormat<LongWritable, DbInputRecord>(
                new DBInputFormat(), LongWritable.class, clazz, job);

        // Get the Hadoop Configuration... which is obtained through the HADOOP_CONF_DIR found in the
        // flink-conf.yaml file. Use it to apply additional configuration, as needed
        Configuration hadoopConfiguration = hadoopInputFormat.getJobConf();

        // Add database configuration to Hadoop Configuration
        DBConfiguration.configureDB(hadoopConfiguration, dbInputRecord.driver(),
            dbInputRecord.url(),
            dbInputRecord.user(), dbInputRecord.password());

        // Provide information regarding source... where/what data will be fetched/read
        DBInputFormat.setInput(job, clazz, dbInputRecord.table(), null, null, dbInputRecord.fields());

        return hadoopInputFormat;
    }
}
