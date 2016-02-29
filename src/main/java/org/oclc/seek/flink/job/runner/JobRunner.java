/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.runner;

import org.oclc.seek.flink.batch.job.DBImportEntryFindJob;
import org.oclc.seek.flink.batch.job.DBImportHadoopJob;
import org.oclc.seek.flink.batch.job.DBImportIssnlJob;
import org.oclc.seek.flink.batch.job.WordCountJob;
import org.oclc.seek.flink.job.JobContract;
import org.oclc.seek.flink.job.factory.JobFactory;
import org.oclc.seek.flink.stream.job.KafkaToConsoleJob;
import org.oclc.seek.flink.stream.job.KafkaToHdfsJob;
import org.oclc.seek.flink.stream.job.QueryGeneratorToDbToHdfsJob;
import org.oclc.seek.flink.stream.job.SocketToConsoleJob;

/**
 *
 */
public class JobRunner {
    /**
     * @param topologyName
     */
    public void run(final String topologyName) {
        run(topologyName, null);
    }

    /**
     * @param topologyName
     * @param query
     */
    public void run(final String topologyName, final String query) {
        try {
            JobContract jobContract = JobFactory.get(topologyName, query);
            jobContract.execute();
        } catch (InstantiationException | IllegalAccessException e) {
            System.out.println("bad topology requested...");
            System.out.println(e.getMessage());
            usage();
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("execution of topology failed...");
            System.out.println(e.getMessage());
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

        if (args.length == 0 || args.length > 1) {
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
        s.append(DBImportHadoopJob.class.getSimpleName().toLowerCase());
        s.append("\n");
        s.append(DBImportEntryFindJob.class.getSimpleName().toLowerCase());
        s.append("\n");
        s.append(DBImportIssnlJob.class.getSimpleName().toLowerCase());
        s.append("\n");
        s.append(QueryGeneratorToDbToHdfsJob.class.getSimpleName().toLowerCase());
        s.append("\n");

        System.out.println(s.toString());
    }
}
