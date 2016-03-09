/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.function;

import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.oclc.seek.flink.document.SolrDocumentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @param <T>
 */
public class SolrSink<T extends String> extends RichSinkFunction<T> {
    private final Logger LOG = LoggerFactory.getLogger(SolrSink.class);

    private static final long serialVersionUID = 1L;
    private final Map<String, String> config;
    private final SolrDocumentBuilder builder;

    private transient SolrClient solrClient;

    // public static final String CONFIG_KEY_SOLR_TYPE = "solr.type";
    // public static final String SOLR_LOCATION = "solr.location";
    public static final String SOLR_ZK_STRING = "zookeeper.connect";

    /**
     * @param config
     * @param builder
     */
    public SolrSink(final Map<String, String> config, final SolrDocumentBuilder builder) {
        this.config = config;
        this.builder = builder;
    }

    @Override
    public void open(final Configuration configuration) {
        solrClient = createClient();
        // solrClient = createClient(config.get(CONFIG_KEY_SOLR_TYPE));
    }

    /**
     * @param document
     * @throws Exception
     */
    @Override
    public void invoke(final T data) throws Exception {
        // SolrInputDocument solrInputDocument = builder.build(data, getRuntimeContext());
        SolrInputDocument solrInputDocument = builder.build(data);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Emitting Solr Input Doc: {}", solrInputDocument);
        }

        System.out.println(solrInputDocument);
        solrClient.add("kbwc-entry", solrInputDocument, 500);
    }

    @Override
    public void close() {
    }

    /**
     * @param solrType
     * @return an instance of {@link SolrClient}
     */
    // public SolrClient createClient(final String solrType) {
    public SolrClient createClient() {
        // if (solrType.equals("http")) {
        // return new HttpSolrClient(config.get(SOLR_LOCATION));
        // } else if (solrType.equals("cloud")) {
        return new CloudSolrClient(config.get(SOLR_ZK_STRING));
        // } else {
        // if (LOG.isInfoEnabled()) {
        // LOG.error("Invalid Solr type in the configuration");
        // }
        // throw new RuntimeException("Invalid Solr type in the configuration, valid types are standard or cloud");
        // }
    }
}
