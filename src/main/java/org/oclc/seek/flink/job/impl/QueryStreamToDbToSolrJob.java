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
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.oclc.seek.flink.document.KbwcEntryDocument;
import org.oclc.seek.flink.function.DBFetcherCallBack;
import org.oclc.seek.flink.function.DocumentParser;
import org.oclc.seek.flink.function.JsonTextParser;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.record.EntryFind;
import org.oclc.seek.flink.sink.SolrSink;

import scala.collection.mutable.StringBuilder;

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

        /*
         * Query Generator stream
         */
        DataStream<String> queries = env.addSource(new QueryGeneratorStream())
            .name("generator of queries");

        /*
         * Enforces the even distribution over all parallel instances of the following task
         */
        DataStream<EntryFind> records = queries.flatMap(new DBFetcherCallBack())
            .rebalance()
            .name("get db records using callback");

        DataStream<String> jsonRecords = records.map(new JsonTextParser<EntryFind>())
            .name("transform db records into json");

        DataStream<KbwcEntryDocument> documents = jsonRecords.map(new DocumentParser())
            .name("create documents for solr");

        // documents.addSink(new SolrSinkBuilder<KbwcEntryDocument>().build(configMap))
        documents.addSink(new SolrSink<KbwcEntryDocument>(configMap))
        .name("Index to solr sink");;

        env.execute("Receives SQL queries... executes them and then writes to Solr");
    }

    /**
     *
     */
    public static class QueryGeneratorStream implements SourceFunction<String> {
        private static final long serialVersionUID = 1L;
        boolean running = true;
        long i = 1;

        static final String[] hex = {
            "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"
        };

        @Override
        public void run(final SourceContext<String> ctx) throws Exception {
            StringBuilder value;
            for (String h : hex) {
                for (String e : hex) {
                    // for (String x : hex) {
                    // for (String a : hex) {
                    value = new StringBuilder();
                    value.append(h);
                    value.append(e);
                    // value.append(x);
                    // value.append(a);
                    ctx.collect("SELECT * FROM entry_find WHERE id LIKE '" + value + "%'");
                    // }
                    // }
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
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
