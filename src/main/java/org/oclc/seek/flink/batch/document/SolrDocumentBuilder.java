/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.batch.document;

import java.io.Serializable;

import org.apache.solr.common.SolrInputDocument;

/**
 *
 */
public class SolrDocumentBuilder implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * @param data
     * @return an instance of {@link SolrInputDocument}
     */
    public SolrInputDocument build(final String data) {
        SolrInputDocument document = new SolrInputDocument();
        document.addField("data", data);
        // document.addField("data", element.f1);
        // document.addField("id", element.f0);

        return document;
    }
}
