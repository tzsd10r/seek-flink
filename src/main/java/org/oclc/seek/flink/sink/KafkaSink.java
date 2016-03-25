/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.sink;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.oclc.seek.flink.serializer.StringSerializerSchema;

/**
 *
 */
public class KafkaSink {
    /**
     * Concise description of what this class represents.
     */
    public static final String DESCRIPTION = "Writes events onto Kafka";
    /**
     * Generic property variable that represents the topic name
     */
    public static String TOPIC = "kafka.sink.topic";

    private StringSerializerSchema serializer = new StringSerializerSchema();

    private FlinkKafkaProducer<String> sink;

    /**
     * Constructor that makes use of a suffix when one is provided, to build the topic name.
     *
     * @param props
     * @param suffix
     */
    public KafkaSink(final String suffix, final Properties props) {
        String topic = props.getProperty(suffix);

        if (topic == null && valueIsValid(suffix)) {
            topic = props.getProperty(TOPIC + "." + suffix);
        }

        sink = new FlinkKafkaProducer<String>(topic, serializer, props);
    }

    /**
     * Constructor that only takes the properties.
     * A generic topic name will be used.
     *
     * @param props
     */
    // public KafkaSink(final Properties props) {
    // this(null, props);
    // }

    /**
     * Constructor that only takes the properties.
     * A generic topic name will be used.
     *
     * @param props
     */
    // public KafkaSink(final Properties props, String topicName) {
    // sink = new FlinkKafkaProducer<String>(topicName, serializer, props);
    // }

    /**
     * @return an instance of {@link SinkFunction}
     */
    public SinkFunction<String> getSink() {
        return sink;
    }

    private boolean valueIsValid(final String value) {
        return !StringUtils.isBlank(value);
    }
}
