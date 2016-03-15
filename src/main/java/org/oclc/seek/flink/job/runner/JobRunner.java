/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.runner;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.oclc.seek.flink.job.JobContract;
import org.oclc.seek.flink.job.factory.JobFactory;
import org.oclc.seek.flink.job.impl.DBImportEntryFindJob;
import org.oclc.seek.flink.job.impl.DBImportHadoopToHdfsJob;
import org.oclc.seek.flink.job.impl.DBImportIssnlJob;
import org.oclc.seek.flink.job.impl.DbToHdfsJob;
import org.oclc.seek.flink.job.impl.DbToKafkaJob;
import org.oclc.seek.flink.job.impl.HdfsToKafkaJob;
import org.oclc.seek.flink.job.impl.KafkaToConsoleJob;
import org.oclc.seek.flink.job.impl.KafkaToHdfsJob;
import org.oclc.seek.flink.job.impl.KafkaToKafkaJob;
import org.oclc.seek.flink.job.impl.KafkaToSolrJob;
import org.oclc.seek.flink.job.impl.QueryStreamToDbToKafkaJob;
import org.oclc.seek.flink.job.impl.SocketToConsoleJob;
import org.oclc.seek.flink.job.impl.SolrEmitterJob;
import org.oclc.seek.flink.job.impl.WordCountJob;
import org.oclc.seek.flink.job.impl.wordcount.WordcountStreamingJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */

// @SpringBootApplication
public class JobRunner {
    private static Logger LOGGER = LoggerFactory.getLogger(JobRunner.class);
    /**
     * @param topologyName
     * @param query
     */
    public void run(final String topologyName) {
        try {
            JobContract jobContract = JobFactory.get(topologyName);
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            jobContract.execute(env);
        } catch (InstantiationException | IllegalAccessException e) {
            LOGGER.info("Bad topology requested...");
            LOGGER.info(e.getMessage());
            usage();
            e.printStackTrace();
        } catch (Exception e) {
            LOGGER.info("Execution of topology failed...");
            LOGGER.info(e.getMessage());
            usage();
            e.printStackTrace();
        }

    }

    /**
     * @param args
     * @throws ClassNotFoundException
     */
    public static void main(final String[] args) {

        JobRunner runner = new JobRunner();

        if (args.length == 0 || args.length > 2) {
            runner.usage();
        }

        runner.run(args[0]);
    }

    /**
     *
     */
    public void usage() {
        StringBuilder s = new StringBuilder();
        s.append(KafkaToHdfsJob.class.getSimpleName().toLowerCase());
        s.append("\n");
        s.append(KafkaToConsoleJob.class.getSimpleName().toLowerCase());
        s.append("\n");
        s.append(SocketToConsoleJob.class.getSimpleName().toLowerCase());
        s.append("\n");
        s.append(WordCountJob.class.getSimpleName().toLowerCase());
        s.append("\n");
        s.append(DBImportEntryFindJob.class.getSimpleName().toLowerCase());
        s.append("\n");
        s.append(DBImportIssnlJob.class.getSimpleName().toLowerCase());
        s.append("\n");
        s.append(DBImportHadoopToHdfsJob.class.getSimpleName().toLowerCase());
        s.append("\n");
        s.append(HdfsToKafkaJob.class.getSimpleName().toLowerCase());
        s.append("\n");
        s.append(WordcountStreamingJob.class.getSimpleName().toLowerCase());
        s.append("\n");
        s.append(SolrEmitterJob.class.getSimpleName().toLowerCase());
        s.append("\n");
        s.append(DbToHdfsJob.class.getSimpleName().toLowerCase());
        s.append("\n");
        s.append(KafkaToKafkaJob.class.getSimpleName().toLowerCase());
        s.append("\n");
        s.append(DbToKafkaJob.class.getSimpleName().toLowerCase());
        s.append("\n");
        s.append(KafkaToSolrJob.class.getSimpleName().toLowerCase());
        s.append("\n");
        s.append(QueryStreamToDbToKafkaJob.class.getSimpleName().toLowerCase());
        s.append("\n");

        LOGGER.info(s.toString());
    }
}
