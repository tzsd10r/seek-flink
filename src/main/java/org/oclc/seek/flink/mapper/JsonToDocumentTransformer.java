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
package org.oclc.seek.flink.mapper;

import groovy.json.JsonParserType;
import groovy.json.JsonSlurper;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.oclc.seek.flink.document.KbwcEntryDocument;

import com.google.gson.Gson;

/**
 *
 */
public class JsonToDocumentTransformer extends RichMapFunction<String, KbwcEntryDocument> {
    private static final long serialVersionUID = 1L;
    /**
     * Concise description of what this class does.
     */
    public static final String DESCRIPTION = "Transforms JSON text into the appropriate Object";
    /**
     * The default JSON parser is Gson.
     * The only other option at the moment is JsonSlurper (Groovy utility)
     */
    private Gson gson;

    @Override
    public void open(final Configuration configuration) throws Exception {
        super.open(configuration);

        // String msg = "Using Groovy JsonSlurper!!!";
        ParameterTool parameterTool =
            (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

        if (parameterTool.getRequired("json.text.parser").equals("gson")) {
            gson = new Gson();
            // msg = "Using Gson!!!";
        }

        // System.out.println(msg);
    }

    @Override
    public KbwcEntryDocument map(final String json) throws Exception {
        if (gson != null) {
            return gson.fromJson(json, KbwcEntryDocument.class);
        }

        return (KbwcEntryDocument) new JsonSlurper().setType(JsonParserType.INDEX_OVERLAY).parseText(json);
    }
}
