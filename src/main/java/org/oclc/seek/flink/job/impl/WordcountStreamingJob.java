/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.job.impl;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.oclc.seek.flink.job.JobGeneric;

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
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        DataStream<String> lines = env.readTextFile(parameterTool.getRequired("fs.wordcount.source"));

        // DataStream<Tuple2<String, Integer>> words = lines.flatMap(new Tokenizer()).keyBy(0).sum(1);
        DataStream<Word> words =
            lines.flatMap(new Tokenizer())
            .keyBy(0)
            .timeWindow(Time.milliseconds(5000))
            // .countWindow(1000)
            // .apply(new WindowFunction<Tuple2<String, Long>, Tuple3<String, Long, Long>, Tuple,
            // GlobalWindow>() {
            // private static final long serialVersionUID = 1L;
            //
            // @Override
            // public void apply(final Tuple arg0, final GlobalWindow window,
            // final Iterable<Tuple2<String, Long>> values,
            // final Collector<Tuple3<String, Long, Long>> collector) throws Exception {
            // Long elapsedMillis = window.maxTimestamp();
            // collector.collect(new Tuple3<String, Long, Long>("Words: ", elapsedMillis,
            // new Long(Iterables.size(values))));
            // }
            // });

            .apply(new WindowFunction<Tuple2<String, Long>, Word, Tuple, TimeWindow>() {
                private static final long serialVersionUID = 1L;

                @Override
                public void apply(final Tuple windowKey, final TimeWindow window,
                    final Iterable<Tuple2<String, Long>> values, final Collector<Word> collector) throws Exception {
                    Long processingTime = window.getEnd() - window.getStart();
                    String word = values.iterator().next().f0;
                    Long count = new Long(Iterables.size(values));

                    collector.collect(new Word(word, processingTime, count));
                }
            });

        words.writeAsText(parameterTool.getRequired("fs.wordcount.output")).name("Filesystem");

        env.execute("Wordcount streaming");
    }

    /**
     *
     */
    public class Tokenizer implements FlatMapFunction<String, Tuple2<String, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(final String sentence, final Collector<Tuple2<String, Long>> out) throws Exception {
            String[] tokens = sentence.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Long>(token, 1L));
                }
            }
        }
    }

    public class Word {
        private String word;
        private Long processingTime;
        private Long count;

        public Word(final String word, final Long processingTime, final Long count) {
            setWord(word);
            setProcessingTime(processingTime);
            setCount(count);
        }

        public String getWord() {
            return word;
        }

        public void setWord(final String word) {
            this.word = word;
        }

        public Long getProcessingTime() {
            return processingTime;
        }

        public void setProcessingTime(final Long processingTime) {
            this.processingTime = processingTime;
        }

        public Long getCount() {
            return count;
        }

        public void setCount(final Long count) {
            this.count = count;
        }
    }

}
