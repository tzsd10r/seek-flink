/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.factory;

import java.util.HashMap;
import java.util.Map;

import org.oclc.seek.flink.job.JobContract;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.topology.DBImportEntryFindJob;
import org.oclc.seek.flink.topology.DBImportHadoopToHdfsJob;
import org.oclc.seek.flink.topology.DBImportIssnlJob;
import org.oclc.seek.flink.topology.DbToHdfsJob;
import org.oclc.seek.flink.topology.DbToKafkaJob;
import org.oclc.seek.flink.topology.HdfsToKafkaJob;
import org.oclc.seek.flink.topology.KafkaToConsoleJob;
import org.oclc.seek.flink.topology.KafkaToHdfsJob;
import org.oclc.seek.flink.topology.KafkaToKafkaJob;
import org.oclc.seek.flink.topology.SocketToConsoleJob;
import org.oclc.seek.flink.topology.SolrEmitterJob;
import org.oclc.seek.flink.topology.WordCountJob;
import org.oclc.seek.flink.topology.WordcountStreamingJob;

/**
 *
 */
public class JobFactory {
    private static Map<String, Class<?>> topologies = new HashMap<String, Class<?>>();
    static {
        topologies.put(KafkaToHdfsJob.class.getSimpleName().toLowerCase(), KafkaToHdfsJob.class);
        topologies.put(KafkaToConsoleJob.class.getSimpleName().toLowerCase(), KafkaToConsoleJob.class);
        topologies.put(SocketToConsoleJob.class.getSimpleName().toLowerCase(), SocketToConsoleJob.class);
        topologies.put(WordCountJob.class.getSimpleName().toLowerCase(), WordCountJob.class);
        topologies.put(DBImportEntryFindJob.class.getSimpleName().toLowerCase(), DBImportEntryFindJob.class);
        topologies.put(DBImportIssnlJob.class.getSimpleName().toLowerCase(), DBImportIssnlJob.class);
        topologies.put(DBImportHadoopToHdfsJob.class.getSimpleName().toLowerCase(), DBImportHadoopToHdfsJob.class);
        topologies.put(HdfsToKafkaJob.class.getSimpleName().toLowerCase(), HdfsToKafkaJob.class);
        topologies.put(WordcountStreamingJob.class.getSimpleName().toLowerCase(), WordcountStreamingJob.class);
        topologies.put(SolrEmitterJob.class.getSimpleName().toLowerCase(), SolrEmitterJob.class);
        topologies.put(DbToHdfsJob.class.getSimpleName().toLowerCase(), DbToHdfsJob.class);
        topologies.put(KafkaToKafkaJob.class.getSimpleName().toLowerCase(), KafkaToKafkaJob.class);
        topologies.put(DbToKafkaJob.class.getSimpleName().toLowerCase(), DbToKafkaJob.class);
    }

    /**
     * @param topologyName
     * @param query
     * @return a JobContract
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public static JobContract get(final String topologyName, final String query) throws InstantiationException,
    IllegalAccessException {
        JobGeneric jobContract = null;

        Class<?> topologyClass = topologies.get(topologyName);

        if (topologyClass == null) {
            throw new IllegalArgumentException("the requested topology is invalid... " + topologyName);
        }

        jobContract = (JobGeneric) topologyClass.newInstance();

        if (query != null) {
            jobContract.init(query);
            jobContract.init();
        } else {
            jobContract.init();
        }

        return jobContract;
    }

    /**
     * @param topologyName
     * @return a JobContract
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public static JobContract get(final String topologyName) throws InstantiationException, IllegalAccessException {
        return get(topologyName, null);
    }
}
