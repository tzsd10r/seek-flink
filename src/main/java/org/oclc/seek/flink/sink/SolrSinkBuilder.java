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

/**
 * @param <T>
 */
public class SolrSinkBuilder<T> {

    /**
     * @param config
     * @return an instance of {@link SolrSink}
     * @throws IOException
     */
    public SolrSink<T> build(final Map<String, String> config) throws IOException {
        return new SolrSink<T>(config);
    }
}
