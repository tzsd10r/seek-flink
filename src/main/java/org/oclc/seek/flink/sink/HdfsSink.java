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
import org.apache.flink.streaming.connectors.fs.NonRollingBucketer;
import org.apache.flink.streaming.connectors.fs.RollingSink;
import org.apache.flink.streaming.connectors.fs.StringWriter;

/**
 *
 */
public class HdfsSink {
    /**
     * Concise description of what this class represents.
     */
    public static final String DESCRIPTION = "Writes text to HDFS.";
    /**
     * Generic property variable that represents the topic name
     */
    public static String DIR = "fs.sink.dir";

    private RollingSink<String> sink;

    /**
     * Constructor that makes use of a suffix when one is provided, to build the topic name.
     *
     * @param props
     * @param suffix
     */
    public HdfsSink(final String suffix, final Properties props) {
        this(init(suffix, props));
    }

    /**
     * Constructor that only takes the properties.
     * A generic topic name will be used.
     *
     * @param path
     */
    public HdfsSink(final String path) {
        sink = new RollingSink<String>(path);
    }

    /**
     * @return an instance of {@link SinkFunction}
     */
    public SinkFunction<String> getSink() {
        sink.setBucketer(new NonRollingBucketer());
        sink.setPendingSuffix("");
        sink.setWriter(new StringWriter<String>());
        return sink;
    }

    private static boolean valueIsValid(final String value) {
        return !StringUtils.isBlank(value);
    }

    private static String init(final String suffix, final Properties props) {
        String path = props.getProperty(DIR);

        if (path == null && valueIsValid(suffix)) {
            path = props.getProperty(DIR + "." + suffix);
        }

        return path;
    }

}
