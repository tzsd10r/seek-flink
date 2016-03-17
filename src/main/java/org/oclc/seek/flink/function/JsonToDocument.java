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
package org.oclc.seek.flink.function;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.oclc.seek.flink.builder.DocumentBuilder;

/**
 * @param <T>
 */
public class JsonToDocument<T> extends RichFlatMapFunction<String, T> {
    private static final long serialVersionUID = 1L;
    private LongCounter recordCount = new LongCounter();
    private DocumentBuilder<T> builder;

    @Override
    public void open(final Configuration parameters) throws Exception {
        super.open(parameters);
        getRuntimeContext().addAccumulator("recordCount", recordCount);
        builder = new DocumentBuilder<T>();
    }

    @Override
    public void flatMap(final String json, final Collector<T> collector) throws Exception {
        // EntryFind entryFind = new EntryFind().fromJson(json);
        recordCount.add(1L);
        // collector.collect(builder.build(entryFind));
        // return builder.build(entryFind, KbwcEntryDocument.class);
    }
}