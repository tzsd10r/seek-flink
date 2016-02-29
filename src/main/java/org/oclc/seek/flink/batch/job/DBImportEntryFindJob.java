/****************************************************************************************************************
 * Copyright (c) 2016 OCLC, Inc. All Rights Reserved.
 * OCLC proprietary information: the enclosed materials contain
 * proprietary information of OCLC, Inc. and shall not be disclosed in whole or in
 * any part to any third party or used by any person for any purpose, without written
 * consent of OCLC, Inc. Duplication of any portion of these materials shall include his notice.
 ******************************************************************************************************************/

package org.oclc.seek.flink.batch.job;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.builder.Tuple2Builder;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;
import org.oclc.seek.flink.batch.source.JDBCSource;
import org.oclc.seek.flink.job.JobContract;
import org.oclc.seek.flink.job.JobGeneric;

import com.google.gson.Gson;

/**
 *
 */
public class DBImportEntryFindJob extends JobGeneric implements JobContract {
    private Properties props = new Properties();

    @Override
    public void init(final String query) {
        props.put("query", query);
    }
    /**
     *
     */
    @Override
    public void init() {
        props.put("zookeeper.connect",
            "ilabhddb03dxdu.dev.oclc.org:9011,ilabhddb04dxdu.dev.oclc.org:9011,ilabhddb05dxdu.dev.oclc.org:9011");
        props.put("hdfs.folder", "/user/seabrae/flink");
        props.put("hdfs.host", "hdfs://ilabhddb02dxdu.dev.oclc.org:9008");

        props.put("driver", "com.mysql.jdbc.Driver");
        props.put("table", "entry_find");
        props.put("fields", "owner_institution, collection_uid");
        props.put("url", "jdbc:mysql://wmsdev-vip3.ent.oclc.org:3306/kbwc_seek");
        props.put("user", "kbwc_seek");
        props.put("password", "kbwc_seek");
        props.put("conditions", "limit 100");

        // private String url =
        // "jdbc:mysql://mysqlwms-vip2.pte.oclc.org:3306/kbwc_prod";
        // "jdbc:mysql://mysqlwms-vip2.pte.oclc.org:3306/kbwc_prod?sessionVariables=wait_timeout=43200&amp;useUnicode=true&amp;characterEncoding=UTF8&amp;serverTimezone=UTC&amp;useLegacyDatetimeCode=false";
        // private String user = "kbwc_user";
        // private String password = "ZGnfwUo6i1";

        parameterTool = ParameterTool.fromMap(propertiesToMap(props));
    }

    @Override
    public void execute() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setNumberOfExecutionRetries(-1);
        env.getConfig().setParallelism(2);
        // make configuration available globally in all functions
        env.getConfig().setGlobalJobParameters(parameterTool);

        // ConfigConstants.AKKA_FRAMESIZE, 20000000

        /*
         * Flink's program compiler needs to infer the data types of the data items which are returned
         * by an InputFormat. If this information cannot be automatically inferred, it is necessary to
         * manually provide the type information as shown below.
         */
        TupleTypeInfo tupleTypeInfo = new TupleTypeInfo(Tuple2.class, BasicTypeInfo.LONG_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO);

        DataSet<Tuple2<Long, String>> output =
            env.createInput(new JDBCSource<Tuple2<Long, String>>().build(parameterTool), tupleTypeInfo);

        output.flatMap(new RichFlatMapFunction<Tuple2<Long, String>, String>() {
            private static final long serialVersionUID = 1L;
            private String[] fields;


            @Override
            public void open(final Configuration parameters) throws Exception {
                fields = parameters.getString("fields", null).split(",");
            }

            @Override
            public void flatMap(final Tuple2<Long, String> tuple, final Collector<String> output) throws Exception {
                Map<String, Object> map = new HashMap<String, Object>();

                String k0 = fields[0].trim();
                String k1 = fields[1].trim();

                Object v0 = tuple.f0;
                Object v1 = tuple.f1;

                Tuple2<String, Object>[] tupleArray = new Tuple2Builder<String, Object>()
                    .add(k0, v0)
                    .add(k1, v1)
                    .build();

                for (Tuple2<String, Object> t : tupleArray) {
                    map.put(t.f0, t.f1);
                }

                output.collect(new Gson().toJson(map));
            }
        }).withParameters(parameterTool.getConfiguration())
        .writeAsText("hdfs:///" + parameterTool.get("hdfs.folder") + "/result.txt", WriteMode.OVERWRITE)
        .name("hdfs");

        env.execute("Fetching Data from Database");
    }
}
