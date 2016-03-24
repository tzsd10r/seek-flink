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
package org.oclc.seek.flink.mapper;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * @param <T>
 */
public class RecordsCounter<T> extends RichMapFunction<T, T> {
    private static final long serialVersionUID = 1L;
    /**
     * Concise description of what this class represents.
     */
    public static final String DESCRIPTION = "Counts records";
    private LongCounter recordCount = new LongCounter();

    @Override
    public void open(final Configuration parameters) throws Exception {
        super.open(parameters);
        getRuntimeContext().addAccumulator("recordCount", recordCount);
    }

    @Override
    public T map(final T obj) throws Exception {
        recordCount.add(1L);
        return obj;
    }
}
