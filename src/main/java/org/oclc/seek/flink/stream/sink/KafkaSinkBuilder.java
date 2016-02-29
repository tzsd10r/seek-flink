/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.stream.sink;

import java.util.Properties;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 *
 */
public class KafkaSinkBuilder {
    /**
     * @param topic
     * @param properties
     * @return an instance of {@link FlinkKafkaProducer}
     */
    @SuppressWarnings({
        "unchecked", "rawtypes"
    })
    public SinkFunction<String> build(final String topic, final Properties properties) {
        return new FlinkKafkaProducer(topic, new SimpleStringSchema(), properties);

    }
}
