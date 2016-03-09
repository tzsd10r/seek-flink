/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.topology;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.oclc.seek.flink.job.JobContract;
import org.oclc.seek.flink.job.JobGeneric;

/**
 *
 */
public class WordcountStreamingJob extends JobGeneric implements JobContract {

    @Override
    public void init() {
        super.init();
    }

    @Override
    public void execute(final StreamExecutionEnvironment env) throws Exception {
        DataStream<String> text = env.readTextFile(parameterTool.getRequired("hdfs.wordcount.source"));

        DataStream<Tuple2<String, Integer>> transformed = text.flatMap(new Tokenizer()).keyBy(0).sum(1);

        transformed.writeAsText(parameterTool.getRequired("hdfs.wordcount.output"));

        env.execute("Wordcount Streaming");
    }

    /**
     *
     */
    public class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(final String sentence, final Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = sentence.toLowerCase().split("\\W+");
            // System.out.println("tokens: " + Arrays.toString(tokens));
            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(new String(token), new Integer(1)));
                }
            }
        }

    }
}
