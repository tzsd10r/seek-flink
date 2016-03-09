
package org.oclc.seek.flink.job.impl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.oclc.seek.flink.builder.KbwcDocumentBuilder;
import org.oclc.seek.flink.document.KbwcEntryDocument;
import org.oclc.seek.flink.indexer.SeekIndexer;
import org.oclc.seek.flink.job.JobGeneric;
import org.oclc.seek.flink.record.EntryFind;
import org.oclc.seek.flink.sink.SolrSink;
import org.oclc.seek.flink.sink.SolrSinkBuilder;
import org.oclc.seek.flink.source.KafkaSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

/**
 *
 */
public class KafkaToSolr extends JobGeneric {
    /**
     *
     */
    public static Logger LOGGER = LoggerFactory.getLogger(KafkaToSolr.class);

    @Override
    public void init() {
        super.init();
    }

    /**
     * @throws Exception
     */
    @Override
    public void execute(final StreamExecutionEnvironment env) throws Exception {
        // create a checkpoint every 5 seconds
        // env.enableCheckpointing(5000);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        final Map<String, String> solrConfig = new HashMap<String, String>();
        solrConfig.put(SolrSink.SOLR_ZK_STRING, parameterTool.getRequired(SolrSink.SOLR_ZK_STRING));

        // DataStream<String> jsonRecords = env.addSource(new KafkaSourceBuilder().build(
        // parameterTool.get(prefix + ".kafka.src.topic"),
        // parameterTool.getProperties()));

        DataStream<String> jsonRecords = env.addSource(new KafkaSourceBuilder().build(
            Config.TOPIC_INDEX_INPUT,
            Config.PROP_KAFKA));


        // Define the desired time window
        // WindowedStream<T, K, Window>

        DataStream<KbwcEntryDocument> docs = jsonRecords
            .map(new RichMapFunction<String, KbwcEntryDocument>() {
                private static final long serialVersionUID = 1L;
                private LongCounter recordCount = new LongCounter();

                @Override
                public void open(final Configuration parameters) throws Exception {
                    super.open(parameters);
                    getRuntimeContext().addAccumulator("recordCount", recordCount);
                }

                @Override
                public KbwcEntryDocument map(final String record) throws Exception {
                    Gson g = new Gson();
                    EntryFind ef = g.fromJson(record, EntryFind.class);
                    recordCount.add(1L);
                    return KbwcDocumentBuilder.build(ef);
                }
            }).name("convert json into documents and count the records");
        /*
         * This operation can be inherently non-parallel since all elements have to pass through
         * the same operator instance.
         */
        // .windowAll(TumblingTimeWindows.of(Time.milliseconds(100)));
        // .timeWindowAll(Time.milliseconds(100));

        // DataStream<SeekIndexer> dataAnalysis = docs
        // .rebalance()
        // .timeWindowAll(Time.milliseconds(100))
        // .apply(new EchoWindow())
        // .rebalance();

        // dataAnalysis.addSink(new SolrSinkBuilder<SeekIndexer>(config, new SolrDocumentBuilder()))
        docs.addSink(new SolrSinkBuilder<KbwcEntryDocument>().build(solrConfig))
        .name("solr sink");;

        env.execute();
    }

    class EchoWindow implements AllWindowFunction<KbwcEntryDocument, SeekIndexer, TimeWindow> {
        @Override
        public void apply(final TimeWindow window, final Iterable<KbwcEntryDocument> values,
            final Collector<SeekIndexer> out) throws Exception {
            SeekIndexer si = new SeekIndexer("kbwcentry");

            // CloudSolrClient client = new CloudSolrClient("");
            // client.setDefaultCollection("");

            // Gson g = new Gson();

            StopWatch sw = new StopWatch();
            sw.start();

            // List<KbwcEntryDocument> kds = new ArrayList<>();
            // for (KbwcEntryDocument value : values) {
            // EntryFind ef = g.fromJson(value, EntryFind.class);
            // kds.add(KbwcDocumentBuilder.build(ef));
            // }

            // client.addBeans(kds);
            // client.commit();

            sw.stop();
            si.setSize(Iterables.size(values));
            si.setElapsed(sw.getTime());
            out.collect(si);
        }
    }

    public static class Config implements Serializable {
        public static final String TOPIC_LOAD_INPUT = "kbwc.entry.load.input";
        public static final String TOPIC_LOAD_OUTPUT = "kbwc.entry.load.output";
        public static final String TOPIC_INDEX_INPUT = "kbwc.entry.load.output";

        public static final String ZOOKEEPER_HOSTS =
            "ilabhddb03dxdu.dev.oclc.org:9011,ilabhddb04dxdu.dev.oclc.org:9011";
        public static final String SOLR_COLLECTION = "kbwc-entry";
        public static final String SOLR_URL = "http://localhost:8983/solr/collection1";

        public static Properties PROP_KAFKA = new Properties();
        public static Properties PROP_DB = new Properties();

        static {
            PROP_KAFKA.setProperty("bootstrap.servers",
                "ilabhddb03dxdu.dev.oclc.org:9077,ilabhddb04dxdu.dev.oclc.org:9077");
            PROP_KAFKA.setProperty("zookeeper.connect", ZOOKEEPER_HOSTS);
            PROP_KAFKA.setProperty("group.id", "id");

            PROP_DB.put("db.url", "jdbc:mysql://mysqldev-vip2.dev.oclc.org:3306/kbwc_test");
            PROP_DB.put("db.user", "kbwc_user");
            PROP_DB.put("db.password", "kbwc_user");
            PROP_DB.put("db.driver", "com.mysql.jdbc.Driver");
        }
    }

}
