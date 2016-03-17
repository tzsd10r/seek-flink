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
package org.oclc.seek.flink.builder;

import java.io.Serializable;

import org.oclc.seek.flink.record.BaseObject;

/**
 * @param <R>
 */
public interface DocumentBuilderContract<R> extends Serializable {
    /**
     * @param input
     * @param clazz
     * @return an instance of R
     */
    public R build(final BaseObject input, final Class<R> clazz);

    public Object build(final String input, final Class<R> clazz);
}
