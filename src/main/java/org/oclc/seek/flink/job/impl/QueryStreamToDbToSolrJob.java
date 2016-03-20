/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.impl;

import java.util.Map;

import org.apache.flink.shaded.com.google.common.collect.ImmutableMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.oclc.seek.flink.document.KbwcEntryDocument;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.mapper.DBFetcherCallBack;
import org.oclc.seek.flink.mapper.DocumentParser;
import org.oclc.seek.flink.mapper.JsonTextParser;
import org.oclc.seek.flink.record.EntryFind;
import org.oclc.seek.flink.sink.SolrSink;
import org.oclc.seek.flink.source.QueryGeneratorSource;

/**
 * Here, you can start creating your execution plan for Flink.
 * <p>
 * Start with getting some data from the environment, as follows:
 *
 * <pre>
 * env.readTextFile(textPath);
 * </pre>
 *
 * ...then, transform the resulting DataStream<T> using operations like the following:
 * <p>
 * .filter() <br>
 * .flatMap() <br>
 * .join() <br>
 * .group()
 * <p>
 * ...and many more.
 * <p>
 * Have a look at the programming guide and examples:
 * <p>
 * http://flink.apache.org/docs/latest/programming_guide.html<br>
 * http://flink.apache.org/docs/latest/examples.html <br>
 */
public class QueryStreamToDbToSolrJob extends JobGeneric {
    private static final long serialVersionUID = 1L;

    @Override
    public void init() {
        super.init();
    }

    /**
     * @throws Exception
     */
    @Override
    public void execute(final StreamExecutionEnvironment env) throws Exception {
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        String zkHosts = parameterTool.getRequired(SolrSink.ZKHOSTS);
        String collection = parameterTool.getRequired(SolrSink.COLLECTION);
        Map<String, String> configMap = ImmutableMap
            .of(SolrSink.ZKHOSTS, zkHosts, SolrSink.COLLECTION, collection);

        DataStream<String> queries = env.addSource(new QueryGeneratorSource())
            .name(QueryGeneratorSource.DESCRIPTION);

        DataStream<EntryFind> records = queries.flatMap(new DBFetcherCallBack())
            .rebalance()
            .name(DBFetcherCallBack.DESCRIPTION);

        DataStream<String> jsonRecords = records.map(new JsonTextParser<EntryFind>())
            .name(JsonTextParser.DESCRIPTION);

        DataStream<KbwcEntryDocument> documents = jsonRecords.map(new DocumentParser())
            .name(DocumentParser.DESCRIPTION);

        documents.addSink(new SolrSink<KbwcEntryDocument>(configMap))
        .name(SolrSink.DESCRIPTION);;

        env.execute("Receives SQL queries... executes them and then writes to Solr");
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        System.setProperty("environment", "test");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        QueryStreamToDbToSolrJob job = new QueryStreamToDbToSolrJob();
        job.init();
        job.execute(env);
    }
}
