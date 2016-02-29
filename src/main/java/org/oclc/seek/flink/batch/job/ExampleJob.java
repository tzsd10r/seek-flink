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
package org.oclc.seek.flink.batch.job;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class ExampleJob {

    public void complexIntegrationTest2() throws Exception {
        // Testing POJO source, grouping by multiple fields and windowing with timestamp
        String expected1 = "water_melon-b\n" + "water_melon-b\n" + "water_melon-b\n" + "water_melon-b\n" +
            "water_melon-b\n" + "water_melon-b\n" + "water_melon-b\n" + "water_melon-b\n" + "water_melon-b\n" +
            "water_melon-b\n" + "water_melon-b\n" + "water_melon-b\n" + "water_melon-b\n" + "water_melon-b\n" +
            "water_melon-b\n" + "water_melon-b\n" + "water_melon-b\n" + "water_melon-b\n" + "water_melon-b\n" +
            "water_melon-b\n" + "orange-b\n" + "orange-b\n" + "orange-b\n" + "orange-b\n" + "orange-b\n" +
            "orange-b\n" + "orange-c\n" + "orange-c\n" + "orange-c\n" + "orange-c\n" + "orange-d\n" + "orange-d\n" +
            "peach-d\n" + "peach-d\n";

        List<Tuple5<Integer, String, Character, Double, Boolean>> input = Arrays.asList(
            new Tuple5<>(1, "apple", 'j', 0.1, false),
            new Tuple5<>(1, "peach", 'b', 0.8, false),
            new Tuple5<>(1, "orange", 'c', 0.7, true),
            new Tuple5<>(2, "apple", 'd', 0.5, false),
            new Tuple5<>(2, "peach", 'j', 0.6, false),
            new Tuple5<>(3, "orange", 'b', 0.2, true),
            new Tuple5<>(6, "apple", 'c', 0.1, false),
            new Tuple5<>(7, "peach", 'd', 0.4, false),
            new Tuple5<>(8, "orange", 'j', 0.2, true),
            new Tuple5<>(10, "apple", 'b', 0.1, false),
            new Tuple5<>(10, "peach", 'c', 0.5, false),
            new Tuple5<>(11, "orange", 'd', 0.3, true),
            new Tuple5<>(11, "apple", 'j', 0.3, false),
            new Tuple5<>(12, "peach", 'b', 0.9, false),
            new Tuple5<>(13, "orange", 'c', 0.7, true),
            new Tuple5<>(15, "apple", 'd', 0.2, false),
            new Tuple5<>(16, "peach", 'j', 0.8, false),
            new Tuple5<>(16, "orange", 'b', 0.8, true),
            new Tuple5<>(16, "apple", 'c', 0.1, false),
            new Tuple5<>(17, "peach", 'd', 1.0, true));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableTimestamps();

        SingleOutputStreamOperator<Tuple5<Integer, String, Character, Double, Boolean>,
        DataStreamSource<Tuple5<Integer, String, Character, Double, Boolean>>> sourceStream21 =
        env.fromCollection(input);
        DataStream<OuterPojo> sourceStream22 = env.addSource(new PojoSource());

        sourceStream21
        .assignTimestamps(new MyTimestampExtractor())
        .keyBy(2, 2)
        .timeWindow(Time.of(10, TimeUnit.MILLISECONDS), Time.of(4, TimeUnit.MILLISECONDS))
        .maxBy(3)
        .map(new MyMapFunction2()).flatMap(new MyFlatMapFunction())
        .connect(sourceStream22)
        .map(new MyCoMapFunction())
        .writeAsText("", FileSystem.WriteMode.OVERWRITE);

        env.execute();
    }

    // *************************************************************************
    // FUNCTIONS
    // *************************************************************************

    private static class MyMapFunction2 implements
    MapFunction<Tuple5<Integer, String, Character, Double, Boolean>, Tuple4<Integer, String, Double, Boolean>> {

        @Override
        public Tuple4<Integer, String, Double, Boolean> map(final Tuple5<Integer, String, Character, Double, Boolean> value)
            throws Exception {
            return new Tuple4<>(value.f0, value.f1 + "-" + value.f2, value.f3, value.f4);
        }

    }

    private static class PojoSource implements SourceFunction<OuterPojo> {
        private static final long serialVersionUID = 1L;

        long cnt = 0;

        @Override
        public void run(final SourceContext<OuterPojo> ctx) throws Exception {
            for (int i = 0; i < 20; i++) {
                OuterPojo result = new OuterPojo(new InnerPojo(cnt / 2, "water_melon-b"), 2L);
                ctx.collect(result);
            }
        }

        @Override
        public void cancel() {

        }
    }

    private static class MyTimestampExtractor implements
    TimestampExtractor<Tuple5<Integer, String, Character, Double, Boolean>> {
        private static final long serialVersionUID = 1L;

        @Override
        public long extractTimestamp(final Tuple5<Integer, String, Character, Double, Boolean> value,
            final long currentTimestamp) {
            return value.f0;
        }

        @Override
        public long extractWatermark(final Tuple5<Integer, String, Character, Double, Boolean> value,
            final long currentTimestamp) {
            return (long) value.f0 - 1;
        }

        @Override
        public long getCurrentWatermark() {
            return Long.MIN_VALUE;
        }
    }

    private static class MyFlatMapFunction implements
    FlatMapFunction<Tuple4<Integer, String, Double, Boolean>, OuterPojo> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(final Tuple4<Integer, String, Double, Boolean> value, final Collector<OuterPojo> out)
            throws Exception {
            if (value.f3) {
                for (int i = 0; i < 2; i++) {
                    out.collect(new OuterPojo(new InnerPojo((long) value.f0, value.f1), (long) i));
                }
            }
        }
    }

    private class MyCoMapFunction implements CoMapFunction<OuterPojo, OuterPojo, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String map1(final OuterPojo value) {
            return value.f0.f1;
        }

        @Override
        public String map2(final OuterPojo value) {
            return value.f0.f1;
        }
    }

    // Nested class serialized with Kryo
    public static class OuterPojo {
        public InnerPojo f0;
        public Long f1;

        public OuterPojo(final InnerPojo f0, final Long f1) {
            this.f0 = f0;
            this.f1 = f1;
        }

        @Override
        public String toString() {
            return "POJO(" + f0 + "," + f1 + ")";
        }
    }

    // Flink Pojo
    public static class InnerPojo {
        public Long f0;
        public String f1;

        // default constructor to qualify as Flink POJO
        InnerPojo() {
        }

        public InnerPojo(final Long f0, final String f1) {
            this.f0 = f0;
            this.f1 = f1;
        }

        @Override
        public String toString() {
            return "POJO(" + f0 + "," + f1 + ")";
        }
    }

    private static class TupleSource implements SourceFunction<Tuple2<Long, Tuple2<String, Long>>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void run(final SourceContext<Tuple2<Long, Tuple2<String, Long>>> ctx) throws Exception {
            for (int i = 0; i < 20; i++) {
                Tuple2<Long, Tuple2<String, Long>> result = new Tuple2<>(1L, new Tuple2<>("a", 1L));
                ctx.collect(result);
            }
        }

        @Override
        public void cancel() {

        }
    }



}
