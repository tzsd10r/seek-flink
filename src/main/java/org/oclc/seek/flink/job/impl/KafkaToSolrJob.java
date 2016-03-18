/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.impl;

import java.util.Map;

import org.apache.flink.shaded.com.google.common.collect.ImmutableMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.oclc.seek.flink.document.KbwcEntryDocument;
import org.oclc.seek.flink.function.DocumentParser;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.sink.SolrSink;
import org.oclc.seek.flink.source.KafkaSource;

/**
 *
 */
public class KafkaToSolrJob extends JobGeneric {
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

        String zkHosts = parameterTool.getRequired(SolrSink.ZKHOSTS);
        String collection = parameterTool.getRequired(SolrSink.COLLECTION);
        Map<String, String> configMap = ImmutableMap
            .of(SolrSink.ZKHOSTS, zkHosts, SolrSink.COLLECTION, collection);

        String suffix = parameterTool.getRequired("db.table");

        DataStream<String> jsonRecords =
            env.addSource(new KafkaSource(suffix, parameterTool.getProperties()).getSource())
            .name(KafkaSource.DESCRIPTION);

        DataStream<KbwcEntryDocument> documents = jsonRecords.map(new DocumentParser())
            .name(DocumentParser.DESCRIPTION);

        documents.addSink(new SolrSink<KbwcEntryDocument>(configMap))
        .name(SolrSink.DESCRIPTION);

        env.execute("Reads from Kafka and indexes to Solr");
    }
}
