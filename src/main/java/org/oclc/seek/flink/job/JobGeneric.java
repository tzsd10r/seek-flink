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
package org.oclc.seek.flink.job;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;

/**
 *
 */
public abstract class JobGeneric implements JobContract {
    protected ParameterTool parameterTool;

    /**
     * @param query
     */
    public void init(final String query) {
    }

    @Override
    public void init() {
        Properties props = new Properties();
        props.put("zookeeper.connect",
            "ilabhddb03dxdu.dev.oclc.org:9011,ilabhddb04dxdu.dev.oclc.org:9011,ilabhddb05dxdu.dev.oclc.org:9011");
        props.put("hdfs.folder", "/user/seabrae/flink");
        props.put("hdfs.host", "hdfs://ilabhddb02dxdu.dev.oclc.org:9008");

        parameterTool = ParameterTool.fromMap(propertiesToMap(props));
    }

    /**
     * @param props
     * @return an {@link Map} instance of a {@link Properties} object
     */
    public Map<String, String> propertiesToMap(final Properties props) {
        Map<String, String> map = new HashMap<String, String>();
        for (String key : props.stringPropertyNames()) {
            map.put(key, props.getProperty(key));
        }
        return map;
    }

    @Override
    public abstract void execute() throws Exception;
}
