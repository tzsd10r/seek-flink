/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.sink;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.com.google.common.base.Preconditions;
import org.apache.flink.shaded.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.com.google.common.collect.Iterators;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @param <T>
 */
public class SolrSink<T> extends RichSinkFunction<T> {
    private final Logger LOGGER = LoggerFactory.getLogger(SolrSink.class);
    private static final long serialVersionUID = 1L;

    private Map<String, String> config;
    /**
     * Since {@link CloudSolrClient} isn't serializable we must qualify it as transient
     */
    private transient CloudSolrClient solrClient;

    /**
     * Property that represents the collection
     */
    public static final String COLLECTION = "solr.collection";
    /**
     * Property that represents the zookeeper hosts.
     */
    public static final String ZKHOSTS = "solr.zookeeper.connect";

    /**
     * Concise description of what this class does.
     */
    public static String DESCRIPTION = "Writes documents to a Solr collection";

    /**
     * @param config
     */
    public SolrSink(final Map<String, String> config) {
        isValid(config, "config");

        // Create a local CloudSolrClient to ensure locally that we have required config values
        // Also... ensure we can connect and ping
        try {
            CloudSolrClient client = new CloudSolrClient(config.get(ZKHOSTS));
            client.setDefaultCollection(config.get(COLLECTION));
            client.connect();
            client.ping();
            client.close();
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.config = config;
    }

    /**
     * @param zkHosts
     * @param collection
     */
    public SolrSink(final String zkHosts, final String collection) {
        this(ImmutableMap.of(ZKHOSTS, isValid(zkHosts, "zkHosts"), COLLECTION, isValid(collection, "collection")));
    }

    /**
     * @param value
     * @param name
     * @return the same object received as argument... if not null
     */
    public static <OBJ> OBJ isValid(final OBJ value, final String name) {
        return Preconditions.checkNotNull(value, name + " is not set");
    }

    @Override
    public void open(final Configuration configuration) {
        this.solrClient = new CloudSolrClient(config.get(ZKHOSTS));
        solrClient.setDefaultCollection(config.get(COLLECTION));

        LOGGER.info("Starting Solr Client to index into collection... [{}]", config.get(COLLECTION));
    }

    @Override
    public void invoke(final T obj) throws Exception {
        if (obj instanceof Iterable<?>) {
            Iterator<?> it = ((Iterable<?>) obj).iterator();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Pushing " + Iterators.size(it) + " docs into Solr for collection: [{}]",
                    config.get(COLLECTION));
            }
            solrClient.addBeans(it);
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Pushing doc into Solr for collection: [{}] \n [{}] ", config.get(COLLECTION), obj);
            }
            solrClient.addBean(obj);
        }

    }

    @Override
    public void close() throws Exception {
        if (solrClient != null) {
            solrClient.close();
        }

        // make sure we propagate pending errors
        // checkErroneous();
    }
}
