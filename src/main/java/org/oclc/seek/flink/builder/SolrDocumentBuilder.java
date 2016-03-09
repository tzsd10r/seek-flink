/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.builder;

import java.io.Serializable;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

/**
 * @param <T>
 */
public class SolrDocumentBuilder<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * @param fields
     * @return an instance of {@link SolrInputDocument}
     */
    public SolrInputDocument build(final Map<String, SolrInputField> fields) {
        return new SolrInputDocument(fields);
    }

    /**
     * @param data
     * @return an instance of {@link SolrInputDocument}
     */
    @SuppressWarnings("unchecked")
    public SolrInputDocument build(final T data) {
        if (data instanceof Map) {
            return build((Map<String, SolrInputField>) data);
        }

        SolrInputDocument document = new SolrInputDocument();
        document.addField("data", data);
        return document;
    }
}
