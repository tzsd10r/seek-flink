
package org.oclc.seek.flink.source;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;
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
public class KafkaSource {
    /**
     * Concise description of what this class does.
     */
    public static String DESCRIPTION = "Listens to Kafka topic for events.";
    /**
     * Generic property variable that represents the topic name
     */
    public static String TOPIC = "kafka.src.topic";

    private SimpleStringSchema schema = new SimpleStringSchema();

    private FlinkKafkaConsumer082<String> source;

    /**
     * Constructor that makes use of a suffix when one is provided, to build the topic name.
     *
     * @param props
     * @param suffix
     */
    public KafkaSource(final String suffix, final Properties props) {
        String topic = props.getProperty(TOPIC);

        if (topic == null && valueIsValid(suffix)) {
            topic = props.getProperty(TOPIC + "." + suffix);
        }

        source = new FlinkKafkaConsumer082<String>(topic, schema, props);
    }

    /**
     * Constructor that only takes the properties.
     * A generic topic name will be used.
     *
     * @param props
     */
    // public FlinkKafkaConsumer082(final Properties props) {
    // this(null, props);
    // }

    /**
     * Constructor that only takes the properties.
     * A generic topic name will be used.
     *
     * @param props
     */
    // public FlinkKafkaConsumer082(final Properties props, String topicName) {
    // sink = new FlinkKafkaConsumer082<String>(topicName, schema, props);
    // }

    /**
     * @return an instance of {@link FlinkKafkaConsumer082}
     */
    public FlinkKafkaConsumer082<String> getSource() {
        return source;
    }

    private boolean valueIsValid(final String value) {
        return !StringUtils.isBlank(value);
    }
}
