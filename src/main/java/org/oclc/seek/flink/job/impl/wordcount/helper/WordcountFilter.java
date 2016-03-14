/****************************************************************************************************************
 *
 *  Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 *
 *  OCLC proprietary information: the enclosed materials contain
 *  proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 *  any part to any third party or used by any person for any purpose, without written
 *  consent of OCLC, Inc.  Duplication of any portion of these  materials shall include his notice.
 *
 ******************************************************************************************************************/
package org.oclc.seek.flink.job.impl.wordcount.helper;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * It returns anything other than:<br>
 * - null
 * - blank
 * - empty
 * - spaces
 */
public class WordcountFilter implements FilterFunction<String> {
    private static final long serialVersionUID = 1L;

    @Override
    public boolean filter(final String value) throws Exception {
        return !StringUtils.isBlank(value);
    }
}
