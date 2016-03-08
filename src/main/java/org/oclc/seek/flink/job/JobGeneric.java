/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
    public abstract void init();

    /**
     * @param props
     * @return an {@link Map} instance of a {@link Properties} object
     */
    public Map<String, String> propertiesToMap(final Properties props) {
        String mapTasks = System.getProperty("map.tasks");

        Map<String, String> map = new HashMap<String, String>();
        for (String key : props.stringPropertyNames()) {
            map.put(key, props.getProperty(key));
        }

        if (!StringUtils.isBlank(mapTasks)) {
            map.put("map.tasks", mapTasks);
        }

        return map;
    }

    @Override
    public abstract void execute(StreamExecutionEnvironment env) throws Exception;

}
