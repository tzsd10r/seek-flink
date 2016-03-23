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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

/**
 *
 */
public abstract class JobGeneric implements JobContract {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobGeneric.class);
    private static final long serialVersionUID = 1L;
    protected ParameterTool parameterTool;

    @Override
    public void init() {
        // String LOGGER_PREFIX = "log4j.logger.";
        //
        // for (String propertyName : System.getProperties().stringPropertyNames()) {
        // if (propertyName.startsWith(LOGGER_PREFIX)) {
        // String loggerName = propertyName.substring(LOGGER_PREFIX.length());
        // String levelName = System.getProperty(propertyName, "");
        // Level level = Level.toLevel(levelName);
        // if (!"".equals(levelName) && !levelName.toUpperCase().equals(level.toString())) {
        // LOGGER.error("Skipping unrecognized log4j log level " + levelName + ": -D" + propertyName + "="
        // + levelName);
        // continue;
        // }
        //
        // LOGGER.info("Setting " + loggerName + " => " + level.toString());
        // // LoggerFactory.getLogger(loggerName).setLevel(level);
        // }
        // }

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
            throw new RuntimeException(
                "Must specify a parser... '-Djson.text.parser=groovy' or '-Djson.text.parser=gson'");
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

    /**
     * Looks into System.properties for props -DLOG.loggerName=LEVEL to set Logback levels at startup
     * If LEVEL is empty (setting -DLOG.loggerName without level), it erases a previously set level and will inherit
     * from parent logger
     */
//    public static class SysPropLogbackConfigurator {
//        public static final String PROP_PREFIX = "LOG.";
//
//        public static void apply() {
//            System.getProperties().stringPropertyNames().stream().filter(name -> name.startsWith(PROP_PREFIX))
//                .forEach(SysPropLogbackConfigurator::applyProp);
//        }
//
//        // force static init. applySysPropsToLogback will be called only once
//        public static void applyOnce() {
//            OnceInitializer.emptyMethodToForceInit();
//        }
//
//        private static void applyProp(final String name) {
//            final String loggerName = name.substring(PROP_PREFIX.length());
//            final String levelStr = System.getProperty(name, "");
//            final Level level = Level.toLevel(levelStr, null);
//            ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(loggerName)).setLevel(level);
//        }
//
//        private static class OnceInitializer {
//            static {
//                apply();
//            }
//
//            static void emptyMethodToForceInit() {
//            }
//        }
//    }
}
