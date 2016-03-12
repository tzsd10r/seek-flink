/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.sink;

import java.util.Map;

/**
 * @param <T>
 */
public class SolrSinkBuilder<T> {

    /**
     * @param solrConfig
     * @return an instance of {@link SolrSink}
     */
    public SolrSink<T> build(final Map<String, String> solrConfig) {
        return new SolrSink<T>(solrConfig);
    }

    /**
     * @param zkHosts
     * @param collection
     * @return an instance of {@link SolrSink}
     */
    public SolrSink<T> build(final String zkHosts, final String collection) {
        return new SolrSink<T>(zkHosts, collection);
    }
}
