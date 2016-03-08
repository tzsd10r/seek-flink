
package org.oclc.seek.flink.stream.source;

import java.util.Properties;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

/**
 *
 */
public class KafkaSourceBuilder {
    /**
     * @param topic
     * @param properties
     * @return an instance of {@link FlinkKafkaConsumer08}
     */
    // public FlinkKafkaConsumer08<String> build(final String topic, final Properties properties) {
    // return new FlinkKafkaConsumer08<String>(topic, new SimpleStringSchema(),
    // properties);
    // }
    public FlinkKafkaConsumer082<String> build(final String topic, final Properties properties) {
        return new FlinkKafkaConsumer082<String>(topic, new SimpleStringSchema(),
            properties);
    }
}
