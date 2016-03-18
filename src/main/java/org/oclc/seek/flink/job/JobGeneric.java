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
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 */
public abstract class JobGeneric implements JobContract {
    private static final long serialVersionUID = 1L;
    protected ParameterTool parameterTool;

    @Override
    public void init() {
        // ClassLoader cl = ClassLoader.getSystemClassLoader();
        //
        // URL[] urls = ((URLClassLoader) cl).getURLs();
        //
        // for (URL url : urls) {
        // System.out.println(url.getFile());
        // }

        Properties props = new Properties();

        String env = System.getProperty("environment");

        String configFile = "conf/config." + env + ".properties";

        if (StringUtils.isBlank(env) || env.equalsIgnoreCase("test")) {
            configFile = "conf/config.test.properties";
        }

        System.out.println("Using this config file... [" + configFile + "]");

        try {
            props.load(ClassLoader.getSystemResourceAsStream(configFile));
        } catch (Exception e) {
            System.out.println("Failed to load the properties file... [" + configFile + "]");
            e.printStackTrace();
            throw new RuntimeException("Failed to load the properties file... [" + configFile + "]");
        }

        String jsonParser = System.getProperty("json.text.parser");

        if (StringUtils.isBlank(jsonParser) || !jsonParser.equals("gson") && !jsonParser.equals("groovy")) {
            throw new RuntimeException("Must specify a parser... 'groovy' or 'gson'");
        }

        props.put("json.text.parser", jsonParser);

        parameterTool = ParameterTool.fromMap(propertiesToMap(props));
    }

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
    public void execute(final StreamExecutionEnvironment env) throws Exception {
    };

    @Override
    public void execute(final ExecutionEnvironment env) throws Exception {
    };
}
