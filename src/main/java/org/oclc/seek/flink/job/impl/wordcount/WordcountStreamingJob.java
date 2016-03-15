/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.impl.wordcount;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.job.impl.wordcount.helper.WordcountTokenizer.WordcountArrayToTupleTokenizer;
import org.oclc.seek.flink.job.impl.wordcount.helper.WordcountTokenizer.WordcountStringToArrayTokenizer;
import org.oclc.seek.flink.job.impl.wordcount.helper.WordcountWindow.WordcountGlobalWindow;

/**
 *
 */
public class WordcountStreamingJob extends JobGeneric {
    private static final long serialVersionUID = 1L;

    @Override
    public void init() {
        super.init();
    }

    @Override
    public void execute(final StreamExecutionEnvironment env) throws Exception {
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // env.setParallelism(1);

        DataStream<String> lines = env.readTextFile(parameterTool.getRequired("fs.wordcount.source") + "/wcount-0001");

        // KeyedStream<String, String> keyed = lines.keyBy(new WordcountKeySelector());

        DataStream<String[]> tokenized = lines.map(new WordcountStringToArrayTokenizer());
        DataStream<Tuple2<String, Integer>> tuplelized = tokenized.flatMap(new WordcountArrayToTupleTokenizer());

        // DataStream<Tuple2<String, Integer>> tuplelized = lines.flatMap(new WordcountStringToTupleTokenizer());

        // Group by the tuple field "0"
        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = tuplelized.keyBy(0);

        // WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> ws = keyed.countWindow(2000000);
        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> ws = keyed.countWindow(1);
        DataStream<Tuple3<String, Integer, Long>> ds = ws.apply(new WordcountGlobalWindow());

        // Sum up tuple field "1"
        // DataStream<Tuple2<String, Integer>> ds = windowedStream.sum(1);

        // WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> ws =
        // keyed.timeWindow(Time.milliseconds(2000));
        // DataStream<Tuple3<String, Integer, Long>> ds = ws.apply(new WordcountTimeWindow())
        // .keyBy(0).countWindow(2000).sum(1);

        ds.writeAsText(parameterTool.getRequired("fs.wordcount.output")).name("Filesystem");

        env.execute("Wordcount streaming");
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        System.setProperty("environment", "local");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        WordcountStreamingJob job = new WordcountStreamingJob();
        job.init();
        job.execute(env);
    }

}
