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
import org.apache.flink.hadoop.shaded.com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientConfigurer;
import org.apache.solr.client.solrj.impl.HttpClientUtil.HttpClientFactory;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @param <T>
 */
public class SolrSink<T> extends RichSinkFunction<T> {
    private final Logger LOGGER = LoggerFactory.getLogger(SolrSinkBuilder.class);
    private static final long serialVersionUID = 1L;

    private Map<String, String> config;
    /**
     * Since {@link CloudSolrClient} isn't serializable we must qualify it as transient
     */
    private transient CloudSolrClient solrClient;
    private static final String COLLECTION = "kbwc-entry";

    /**
     *
     */
    public static final String SOLR_ZK_STRING = "zookeeper.connect";

    /**
     * @param config
     * @param builder
     * @throws IOException
     */
    public SolrSink(final Map<String, String> config) {
        Preconditions.checkNotNull(config, "config not set");

        this.config = config;

        // Create a local CloudSolrClient to ensure locally that we have required config values
        // Also... ensure we can connect and ping
        // try (CloudSolrClient client = new CloudSolrClient(config.get(SOLR_ZK_STRING))) {
        // client.connect();
        // client.ping();
        // client.close();
        // } catch (SolrServerException e) {
        // e.printStackTrace();
        // } catch (IOException e) {
        // e.printStackTrace();
        // }
    }

    @Override
    public void open(final Configuration configuration) {
        this.solrClient = new CloudSolrClient(config.get(SOLR_ZK_STRING), client());
        // this.solrClient = new CloudSolrClient(config.get(SOLR_ZK_STRING));
        solrClient.setDefaultCollection(COLLECTION);

        LOGGER.info("Starting Solr Client to index into collection... [{}]", COLLECTION);
    }

    /**
     *
     */
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

    /**
     * @param config
     * @return
     */
    private DefaultHttpClient client() {
        DefaultHttpClient httpClient = HttpClientFactory.createHttpClient();
        new HttpClientConfigurer().configure(httpClient, new ModifiableSolrParams());
        return httpClient;
    }
}
