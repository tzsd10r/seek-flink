/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.stream.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.fs.DateTimeBucketer;
import org.apache.flink.streaming.connectors.fs.RollingSink;
import org.apache.flink.streaming.connectors.fs.StringWriter;

/**
 *
 */
public class HdfsSinkBuilder {
    /**
     * @param path
     * @return an instance of {@link SinkFunction}
     */
    public SinkFunction<String> build(final String path) {
        RollingSink<String> sink = new RollingSink<String>(path);

        sink.setBucketer(new DateTimeBucketer("yyyy-MM-dd"));
        sink.setWriter(new StringWriter<String>());

        return sink;
    }
}
