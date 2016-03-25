/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.mapper;

import groovy.json.JsonOutput;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import com.google.gson.Gson;

/**
 * @param <IN>
 */
public class ObjectToJsonTransformer<IN> extends RichMapFunction<IN, String> {
    private static final long serialVersionUID = 1L;
    /**
     * Concise description of what what this class does
     */
    public static final String DESCRIPTION = "Transforms an object into JSON";
    /**
     * The default JSON parser is Gson.
     * The only other option at the moment is JsonSlurper (Groovy utility)
     */
    private Gson gson;


    @Override
    public void open(final Configuration configuration) throws Exception {
        super.open(configuration);

        ParameterTool parameterTool =
            (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();


        if (parameterTool.getRequired("json.text.parser").equals("gson")) {
            gson = new Gson();
        }
    }

    @Override
    public String map(final IN object) throws Exception {
        if (gson != null) {
            return gson.toJson(object);
        }

        return JsonOutput.toJson(object);
    }
}
