/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.sink;

import java.io.IOException;
import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.com.google.common.base.Preconditions;
import org.apache.flink.shaded.com.google.common.collect.ImmutableMap;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @param <T>
 */
public class SolrSink<T> extends RichSinkFunction<T> {
    private final Logger LOGGER = LoggerFactory.getLogger(SolrSinkBuilder.class);
    private static final long serialVersionUID = 1L;

    private Map<String, String> solrConfig;
    /**
     * Since {@link CloudSolrClient} isn't serializable we must qualify it as transient
     */
    private transient CloudSolrClient solrClient;

    public static final String COLLECTION = "solr.collection";
    public static final String ZKHOSTS = "solr.zookeeper.connect";

    /**
     * @param solrConfig
     */
    public SolrSink(final Map<String, String> solrConfig) {
        Preconditions.checkNotNull(solrConfig, "solrConfig not set");

        this.solrConfig = solrConfig;

        // Create a local CloudSolrClient to ensure locally that we have required config values
        // Also... ensure we can connect and ping
        try {
            CloudSolrClient client = new CloudSolrClient(solrConfig.get(ZKHOSTS));
            client.setDefaultCollection(COLLECTION);
            client.connect();
            client.ping();
            client.close();
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param zkHosts
     */
    public SolrSink(final String zkHosts) {
        this(ImmutableMap
            .of(ZKHOSTS, zkHosts == null ? Preconditions.checkNotNull(zkHosts, "zkHosts not set") : zkHosts));
    }

    @Override
    public void open(final Configuration configuration) {
        // this.solrClient = new CloudSolrClient(solrConfig.get(ZKHOSTS), client());
        this.solrClient = new CloudSolrClient(solrConfig.get(ZKHOSTS));
        solrClient.setDefaultCollection(COLLECTION);

        LOGGER.info("Starting Solr Client to index into collection... [{}]", COLLECTION);
    }

    @Override
    public void invoke(final T document) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Emitting data into Solr: {}", document);
        }

        solrClient.addBean(document, 500);
    }

    @Override
    public void close() throws Exception {
        if (solrClient != null) {
            solrClient.close();
        }

        // make sure we propagate pending errors
        // checkErroneous();
    }

    // /**
    // * @param config
    // * @return
    // */
    // private DefaultHttpClient client() {
    // DefaultHttpClient httpClient = HttpClientFactory.createHttpClient();
    // new HttpClientConfigurer().configure(httpClient, new ModifiableSolrParams());
    // return httpClient;
    // }
}
