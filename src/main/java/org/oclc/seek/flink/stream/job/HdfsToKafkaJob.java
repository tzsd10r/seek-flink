/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.stream.job;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Properties;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hadoop.shaded.com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.util.NetUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.oclc.seek.flink.job.JobContract;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.stream.sink.KafkaSinkBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class HdfsToKafkaJob extends JobGeneric implements JobContract {
    private Properties props = new Properties();

    @Override
    public void init() {
        String env = System.getProperty("environment");

        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader) cl).getURLs();

        for (URL url : urls) {
            System.out.println(url.getFile());
        }

        String configFile = "conf/config." + env + ".properties";

        // Properties properties = new Properties();
        try {
            props.load(ClassLoader.getSystemResourceAsStream(configFile));
        } catch (Exception e) {
            System.out.println("Failed to load the properties file... [" + configFile + "]");
            e.printStackTrace();
            throw new RuntimeException("Failed to load the properties file... [" + configFile + "]");
        }

        parameterTool = ParameterTool.fromMap(propertiesToMap(props));
    }

    @Override
    public void execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        // create a checkpoint every 5 secodns
        // env.enableCheckpointing(5000);

        DataStream<String> text = env.readTextFile(parameterTool.getRequired("hdfs.kafka.source"));

        text.map(new RichMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            private LongCounter recordCount = new LongCounter();

            @Override
            public void open(final Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("recordCount", recordCount);
            }

            @Override
            public String map(final String value) throws Exception {
                recordCount.add(1L);
                return value;
            }
        }).name("json-records")
        .addSink(
            new FlinkKafkaProducer(parameterTool.get("kafka.topic"), new StringSerializerSchema(), parameterTool
                .getProperties()))
                // .addSink(new KafkaSinkBuilder().build(parameterTool.get("kafka.topic"),
                // parameterTool.getProperties()))
                .name("kafka");

        // transformed.writeAsText("hdfs:///" + parameterTool.getRequired("hdfs.wordcount.output"));

        env.execute("Reads from HDFS and writes to Kafka");
    }

    /*
     * Licensed to the Apache Software Foundation (ASF) under one or more
     * contributor license agreements. See the NOTICE file distributed with
     * this work for additional information regarding copyright ownership.
     * The ASF licenses this file to You under the Apache License, Version 2.0
     * (the "License"); you may not use this file except in compliance with
     * the License. You may obtain a copy of the License at
     * http://www.apache.org/licenses/LICENSE-2.0
     * Unless required by applicable law or agreed to in writing, software
     * distributed under the License is distributed on an "AS IS" BASIS,
     * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     * See the License for the specific language governing permissions and
     * limitations under the License.
     */

    /**
     * Flink Sink to produce data into a Kafka topic.
     * Please note that this producer does not have any reliability guarantees.
     *
     * @param <IN> Type of the messages to write into Kafka.
     */
    public static class FlinkKafkaProducer<IN> extends RichSinkFunction<IN> {

        private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaProducer.class);

        private static final long serialVersionUID = 1L;

        /**
         * Array with the partition ids of the given topicId
         * The size of this array is the number of partitions
         */
        private final int[] partitions;

        /**
         * User defined properties for the Producer
         */
        private final Properties producerConfig;

        /**
         * The name of the topic this producer is writing data to
         */
        private final String topicId;

        /**
         * (Serializable) SerializationSchema for turning objects used with Flink into
         * byte[] for Kafka.
         */
        private final SerializationSchema<IN, byte[]> schema;

        /**
         * User-provided partitioner for assigning an object to a Kafka partition.
         */
        private final KafkaPartitioner partitioner;

        /**
         * Flag indicating whether to accept failures (and log them), or to fail on failures
         */
        private boolean logFailuresOnly;

        // -------------------------------- Runtime fields ------------------------------------------

        /** KafkaProducer instance */
        private transient KafkaProducer<byte[], byte[]> producer;

        /** The callback than handles error propagation or logging callbacks */
        private transient Callback callback;

        /** Errors encountered in the async producer are stored here */
        private transient volatile Exception asyncException;

        /**
         * Creates a FlinkKafkaProducer for a given topic. The sink produces its input to
         * the topic.
         *
         * @param brokerList
         *            Comma separated addresses of the brokers
         * @param topicId
         *            ID of the Kafka topic.
         * @param serializationSchema
         *            User defined serialization schema.
         */
        public FlinkKafkaProducer(final String brokerList, final String topicId,
            final SerializationSchema<IN, byte[]> serializationSchema) {
            this(topicId, serializationSchema, getPropertiesFromBrokerList(brokerList), null);
        }

        /**
         * Creates a FlinkKafkaProducer for a given topic. The sink produces its input to
         * the topic.
         *
         * @param topicId
         *            ID of the Kafka topic.
         * @param serializationSchema
         *            User defined serialization schema.
         * @param producerConfig
         *            Properties with the producer configuration.
         */
        public FlinkKafkaProducer(final String topicId, final SerializationSchema<IN, byte[]> serializationSchema,
            final Properties producerConfig) {
            this(topicId, serializationSchema, producerConfig, null);
        }

        /**
         * The main constructor for creating a FlinkKafkaProducer.
         *
         * @param topicId The topic to write data to
         * @param serializationSchema A serializable serialization schema for turning user objects into a
         *            kafka-consumable byte[]
         * @param producerConfig Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only
         *            required argument.
         * @param customPartitioner A serializable partitioner for assining messages to Kafka partitions.
         */
        public FlinkKafkaProducer(final String topicId, final SerializationSchema<IN, byte[]> serializationSchema,
            final Properties producerConfig, final KafkaPartitioner customPartitioner) {
            Preconditions.checkNotNull(topicId, "TopicID not set");
            Preconditions.checkNotNull(serializationSchema, "serializationSchema not set");
            Preconditions.checkNotNull(producerConfig, "producerConfig not set");
            ClosureCleaner.ensureSerializable(customPartitioner);
            ClosureCleaner.ensureSerializable(serializationSchema);

            this.topicId = topicId;
            this.schema = serializationSchema;
            this.producerConfig = producerConfig;

            // set the producer configuration properties.

            if (!producerConfig.contains(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
                this.producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    ByteArraySerializer.class.getCanonicalName());
            } else {
                LOG.warn("Overwriting the '{}' is not recommended", ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
            }

            if (!producerConfig.contains(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
                this.producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    ByteArraySerializer.class.getCanonicalName());
            } else {
                LOG.warn("Overwriting the '{}' is not recommended", ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
            }

            // create a local KafkaProducer to get the list of partitions.
            // this will also ensure locally that all required ProducerConfig values are set.
            try (KafkaProducer<Void, IN> getPartitionsProd = new KafkaProducer<>(this.producerConfig)) {
                List<PartitionInfo> partitionsList = getPartitionsProd.partitionsFor(topicId);

                this.partitions = new int[partitionsList.size()];
                for (int i = 0; i < partitions.length; i++) {
                    partitions[i] = partitionsList.get(i).partition();
                }
                getPartitionsProd.close();
            }

            if (customPartitioner == null) {
                this.partitioner = new FixedPartitioner();
            } else {
                this.partitioner = customPartitioner;
            }
        }

        // ---------------------------------- Properties --------------------------

        /**
         * Defines whether the producer should fail on errors, or only log them.
         * If this is set to true, then exceptions will be only logged, if set to false,
         * exceptions will be eventually thrown and cause the streaming program to
         * fail (and enter recovery).
         *
         * @param logFailuresOnly The flag to indicate logging-only on exceptions.
         */
        public void setLogFailuresOnly(final boolean logFailuresOnly) {
            this.logFailuresOnly = logFailuresOnly;
        }

        // ----------------------------------- Utilities --------------------------

        /**
         * Initializes the connection to Kafka.
         */
        @Override
        public void open(final Configuration configuration) {
            producer = new org.apache.kafka.clients.producer.KafkaProducer<>(this.producerConfig);

            RuntimeContext ctx = getRuntimeContext();
            partitioner.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks(), partitions);

            LOG.info("Starting FlinkKafkaProducer ({}/{}) to produce into topic {}",
                ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks(), topicId);

            if (logFailuresOnly) {
                callback = new Callback() {

                    @Override
                    public void onCompletion(final RecordMetadata metadata, final Exception e) {
                        if (e != null) {
                            LOG.error("Error while sending record to Kafka: " + e.getMessage(), e);
                        }
                    }
                };
            }
            else {
                callback = new Callback() {
                    @Override
                    public void onCompletion(final RecordMetadata metadata, final Exception exception) {
                        if (exception != null && asyncException == null) {
                            asyncException = exception;
                        }
                    }
                };
            }
        }

        /**
         * Called when new data arrives to the sink, and forwards it to Kafka.
         *
         * @param next
         *            The incoming data
         */
        @Override
        public void invoke(final IN next) throws Exception {
            // propagate asynchronous errors
            checkErroneous();

            byte[] serialized = schema.serialize(next);
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topicId,
                partitioner.partition(next, partitions.length),
                null, serialized);

            producer.send(record, callback);
        }

        @Override
        public void close() throws Exception {
            if (producer != null) {
                producer.close();
            }

            // make sure we propagate pending errors
            checkErroneous();
        }

        // ----------------------------------- Utilities --------------------------

        private void checkErroneous() throws Exception {
            Exception e = asyncException;
            if (e != null) {
                // prevent double throwing
                asyncException = null;
                e.printStackTrace();
                throw new Exception("Failed to send data to Kafka: " + e.getMessage(), e);
            }
        }

        public static Properties getPropertiesFromBrokerList(final String brokerList) {
            String[] elements = brokerList.split(",");

            // validate the broker addresses
            for (String broker : elements) {
                NetUtils.getCorrectHostnamePort(broker);
            }

            Properties props = new Properties();
            props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
            return props;
        }
    }

}
