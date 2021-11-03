package streaming;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import streaming.objects.Bookmark;
import streaming.objects.KeyStroke;
import streaming.objects.Metric;
import streaming.objects.Query;
import streaming.objects.VisitedLink;
import streaming.process.DeduplicationTransformer;
import streaming.process.PageStayTransformer;
import streaming.process.ReferenceTimeProcessor;
import streaming.process.TTLStoreProcessor;
import streaming.process.WritingTimeDeltaTransformer;
import static streaming.utils.AppProperties.*;
import streaming.utils.ArrayListSerde;
import streaming.utils.CustomSerders;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class StreamingProcessing {

  // static final String TEST_VISITEDLINKS_TOPIC = "neurone.visitedlinks";
  // static final String DB_BOOKMARKS_TOPIC = "neurone.bookmarks";
  // static final String DEFAULT_HOST = "localhost";
  // static final String DEDUP_STORE = "dedup-store";
  // static final String TTL_STORE = "ttl-store";
  // static final String WRITTING_TIME_STORE = "writing_time_store";
  // static final String REFERENCE_TIME_STORE = "reference_time_store";
  // static final String LAST_KEYSTROKE_STORE = "last_keystroke_store";
  // static final String TOTALCOVER_STORE="totalcover_store";
  // static final String BMRELEVANT_STORE="bmrelevant_store";
  // static final String PRECISION_STORE="precision_store";

  // static final String DB_KEYSTROKES_TOPIC = "neurone.keystrokes";
  // static final String DB_QUERIES_TOPICS = "neurone.queries";

  public static void main(final String[] args) throws Exception {
    if (args.length == 0 || args.length > 2) {
      throw new IllegalArgumentException("usage: ... <portForRestEndPoint> [<bootstrap.servers> (optional)]");
    }

    final int port = Integer.parseInt(args[0]);

    final Properties streamsConfiguration = new Properties();

    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID_CONFIG);
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, KAFKA_CACHE_MAX_BYTES_BUFFERING_CONFIG);
    streamsConfiguration.put(StreamsConfig.APPLICATION_SERVER_CONFIG, APPLICATION_REST_DEFAULT_HOST + ":" + port);
    // streamsConfiguration.put("default.deserialization.exception.handler",
    // LogAndContinueExceptionHandler.class);

    final KafkaStreams streams = createStreams(streamsConfiguration);

    streams.cleanUp();

    streams.start();

    startRestProxy(streams, APPLICATION_REST_DEFAULT_HOST, port);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        streams.close();

      } catch (final Exception e) {
        // ignored
      }
    }));

  }

  static void startRestProxy(final KafkaStreams streams, final String host, final int port) throws Exception {
    final HostInfo hostInfo = new HostInfo(host, port);
    new StreamingProcessingRestService(streams, hostInfo);
  }

  public static KafkaStreams createStreams(final Properties streamsConfiguration) {

    final StreamsBuilder builder = new StreamsBuilder();

    /** Serdes declaration */

    final Serde<Long> longSerde = Serdes.Long();
    final Serde<String> stringSerde = Serdes.String();
    final Serde<Double> doubleSerde = Serdes.Double();
    final Serde<VisitedLink> visitedLinkSerde = CustomSerders.VisitedLink();
    final Serde<Bookmark> bookmarkSerde = CustomSerders.Bookmark();
    final Serde<Query> querySerde = CustomSerders.Query();
    final Serde<KeyStroke> keystrokesSerde = CustomSerders.Keystrokes();
    final Serde<Metric> metricSerde = CustomSerders.Metric();

    /* Builder state stores */

    // https://kafka-tutorials.confluent.io/finding-distinct-events/kstreams.html
    final StoreBuilder<KeyValueStore<String, Long>> dedupStoreBuilder = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore(STREAMING_STATE_DEDUP_STORE), stringSerde, longSerde);

    final StoreBuilder<KeyValueStore<String, Long>> ttlStoreBuilder = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore(STREAMING_STATE_TTL_STORE), stringSerde, longSerde);

    final StoreBuilder<KeyValueStore<String, ArrayList<Long>>> referenceTimeBuilder = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(STREAMING_STATE_REFERENCE_TIME_STORE), stringSerde,
        new ArrayListSerde<>(longSerde));

    final StoreBuilder<KeyValueStore<String, Long>> lasTimestampBuilder = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(STREAMING_STATE_LAST_KEYSTROKES_STORE), stringSerde, longSerde);

    final StoreBuilder<KeyValueStore<String, Double>> pageStayBuilder = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(STREAMING_STATE_PAGE_STAY_STORE), stringSerde, doubleSerde);

    final StoreBuilder<KeyValueStore<String, Long>> pageSequenceBuilder = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(STREAMING_STATE_PAGE_SEQUENCE_STORE), stringSerde, longSerde);

    builder.addStateStore(dedupStoreBuilder);
    builder.addStateStore(ttlStoreBuilder);
    builder.addStateStore(referenceTimeBuilder);
    builder.addStateStore(lasTimestampBuilder);
    builder.addStateStore(pageStayBuilder);
    builder.addStateStore(pageSequenceBuilder);

    /** Processing graphs for metrics */

    // Totalcover ---

    KStream<String, VisitedLink> visitedLinks = builder.stream(STREAMING_DB_VISITEDLINKS_TOPIC,
        Consumed.with(stringSerde, visitedLinkSerde).withName("visitedlinks_input_topic"));

    ValueTransformerWithKeySupplier<String, VisitedLink, VisitedLink> suplier = new ValueTransformerWithKeySupplier<String, VisitedLink, VisitedLink>() {
      public ValueTransformerWithKey<String, VisitedLink, VisitedLink> get() {
        return new DeduplicationTransformer<String, VisitedLink, String>(STREAMING_STATE_DEDUP_STORE,
            STREAMING_STATE_TTL_STORE, (key, value) -> value.username + value.url, "visitedlink");
      }

    };

    KStream<String, VisitedLink> pageLinks = visitedLinks.filter((key, value) -> value.url.contains("page"),
        Named.as("filter_page_links"));

    KTable<String, Long> totalCoverStore = pageLinks
        .transformValues(suplier, STREAMING_STATE_DEDUP_STORE, STREAMING_STATE_TTL_STORE)
        .filter((k, v) -> v != null, Named.as("filter_not_null_links")).groupByKey()
        .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(STREAMING_STATE_TOTALCOVER_STORE));

    totalCoverStore.toStream().filter((key, value) -> key != null && value != null, Named.as("filter_null_totalcover"))
        .map((key, value) -> KeyValue.pair(key, new Metric(key, (double) value, STREAMING_TOTALCOVER_TOPIC)),
            Named.as("build_metric_totalcover"))
        .to(STREAMING_TOTALCOVER_TOPIC, Produced.with(stringSerde, metricSerde).withName("sink_totalcover_topic"));

    // Bmrelevant ---

    ValueTransformerWithKeySupplier<String, Bookmark, Bookmark> suplier_bookmark = new ValueTransformerWithKeySupplier<String, Bookmark, Bookmark>() {
      public ValueTransformerWithKey<String, Bookmark, Bookmark> get() {
        return new DeduplicationTransformer<String, Bookmark, String>(STREAMING_STATE_DEDUP_STORE,
            STREAMING_STATE_TTL_STORE,
            (key, value) -> String.format("%s,%s,%s", value.username, value.url, value.action), "bookmark");
      }
    };

    KStream<String, Bookmark> bookmarks = builder.stream(STREAMING_DB_BOOKMARKS_TOPIC,
        Consumed.with(stringSerde, bookmarkSerde).withName("bookmark_input_topic"));

    KTable<String, Long> bmRelevantStore = bookmarks.filter((key, value) -> value.relevant && value.userMade)
        .transformValues(suplier_bookmark, STREAMING_STATE_DEDUP_STORE, STREAMING_STATE_TTL_STORE)
        .filter((k, v) -> v != null, Named.as("filter_not_null_boorkmarks"))
        .groupByKey(Grouped.with(stringSerde, bookmarkSerde).withName("group_bookmarks"))
        .aggregate(() -> 0L, (aggKey, newValue, aggValue) -> {

          if (newValue.action.equals("Bookmark")) {
            return aggValue + 1;
          } else {
            return aggValue - 1;
          }

        }, Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(STREAMING_STATE_BMRELEVANT_STORE)
            .withValueSerde(longSerde));

    bmRelevantStore.toStream()
        .filter((key, value) -> key != null && value != null, Named.as("Filter_not_null_bmrelevant"))
        .map((key, value) -> KeyValue.pair(key, new Metric(key, (double) value, STREAMING_BMRELEVANT_TOPIC)),
            Named.as("build_bmrelevant"))
        .to(STREAMING_BMRELEVANT_TOPIC, Produced.with(stringSerde, metricSerde).withName("sink_bmrelevant_topic"));

    // Precision ---

    KTable<String, Double> precisionStore = totalCoverStore.join(bmRelevantStore, (totalcover, bmrelevant) -> {

      if ((double) bmrelevant == 0.0 || (double) totalcover == 0.0) {
        return 0.0;
      } else {
        return (double) bmrelevant / (double) totalcover;
      }
    }, Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as(STREAMING_STATE_PRECISION_STORE)
        .withValueSerde(doubleSerde));

    precisionStore.toStream().filter((key, value) -> key != null && value != null, Named.as("filter_not_nul_precision"))
        .map((key, value) -> KeyValue.pair(key, new Metric(key, value, STREAMING_PRECISION_TOPIC)),
            Named.as("build_precision_metric"))
        .to(STREAMING_PRECISION_TOPIC, Produced.with(stringSerde, metricSerde).withName("sink_precision_topic"));

    // WrtitingTime ---

    ValueTransformerWithKeySupplier<String, KeyStroke, Double> writingTimeDeltasuplier = new ValueTransformerWithKeySupplier<String, KeyStroke, Double>() {
      public ValueTransformerWithKey<String, KeyStroke, Double> get() {
        return new WritingTimeDeltaTransformer<KeyStroke>();
      }
    };

    KStream<String, KeyStroke> keystrokes = builder.stream(STREAMING_DB_KEYSTROKES_TOPIC,
        Consumed.with(stringSerde, keystrokesSerde).withName("keystrokes_input_topic"));

    KTable<String, Double> writingTimeStore = keystrokes
        .filter((key, value) -> value.url.contains("search"), Named.as("filter_keytrsokes_by_origin"))
        .transformValues(writingTimeDeltasuplier, STREAMING_STATE_REFERENCE_TIME_STORE,
            STREAMING_STATE_LAST_KEYSTROKES_STORE)
        .filter((k, v) -> v > 0, Named.as("filter_not_null_delta")).groupByKey()
        .reduce((aggValue, newValue) -> (aggValue + newValue / 1000),
            Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as(STREAMING_STATE_WRITING_TIME_STORE)
                .withValueSerde(doubleSerde));

    writingTimeStore.toStream()
        .filter((key, value) -> key != null && value != null, Named.as("filter_null_writtingtime"))
        .map((key, value) -> KeyValue.pair(key, new Metric(key, (double) value, STREAMING_WRITINGTIME_TOPIC)),
            Named.as("build_metric_writtingtime"))
        .to(STREAMING_WRITINGTIME_TOPIC, Produced.with(stringSerde, metricSerde).withName("sink_writingtime_topic"));

    ProcessorSupplier<String, Long> referenceTimeProcessorSuplier = new ProcessorSupplier<String, Long>() {
      public Processor<String, Long> get() {
        return new ReferenceTimeProcessor(STREAMING_STATE_REFERENCE_TIME_STORE);
      }
    };

    KStream<String, Query> queries = builder.stream(STREAMING_DB_QUERIES_TOPIC,
        Consumed.with(stringSerde, querySerde).withName("query_input_topic"));

    queries.mapValues((k, v) -> v.localTimestamp.longValue())
        .merge(pageLinks.mapValues((k, v) -> v.localTimestamp.longValue()))
        .process(referenceTimeProcessorSuplier, STREAMING_STATE_REFERENCE_TIME_STORE);
    ;

    // Page Stay

    ValueTransformerWithKeySupplier<String, VisitedLink, Double> pageStayTranformerSuplier = new ValueTransformerWithKeySupplier<String, VisitedLink, Double>() {
      public ValueTransformerWithKey<String, VisitedLink, Double> get() {
        return new PageStayTransformer();
      }
    };

    KStream<String, Double> pagestayRaw = pageLinks.transformValues(pageStayTranformerSuplier,
        STREAMING_STATE_PAGE_STAY_STORE, STREAMING_STATE_PAGE_SEQUENCE_STORE)
        .filter((k, v) -> v > 0, Named.as("filter_not_null_pagestay")).mapValues((k,v)->v/1000,Named.as("pagestay_to_second"));

        pagestayRaw
        .map((k, v) -> KeyValue.pair(k, new Metric(k, (double) v, STREAMING_PAGE_STAY_TOPIC)),
            Named.as("build_metric_pagestay"))
        .to(STREAMING_PAGE_STAY_TOPIC, Produced.with(stringSerde, metricSerde).withName("sink_pagestay_topic"));

    KTable<String, Double> totalPagestayStore = pagestayRaw.groupByKey().reduce(
        (aggValue, newValue) -> (aggValue + newValue),
        Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as(STREAMING_STATE_TOTAL_PAGE_STAY_STORE)
            .withValueSerde(doubleSerde));

    totalPagestayStore.toStream()
        .filter((key, value) -> key != null && value != null, Named.as("filter_null_totalpagestay"))
        .map((key, value) -> KeyValue.pair(key, new Metric(key, (double) value, STREAMING_TOTAL_PAGE_STAY_TOPIC)),
            Named.as("build_metric_totalpagestay"))
        .to(STREAMING_TOTAL_PAGE_STAY_TOPIC,
            Produced.with(stringSerde, metricSerde).withName("sink_totalpagestay_topic"));



    // If Quotes


    queries.filter((k,v)->v.query.contains("\"") || v.query.contains("\'"))
    .map((k, v) -> KeyValue.pair(k, new Metric(k, 1D, STREAMING_IF_QUOTES_TOPIC)),
    Named.as("build_metric_ifquotes"))
.to(STREAMING_IF_QUOTES_TOPIC, Produced.with(stringSerde, metricSerde).withName("sink_ifquotes_topic"));
    
            
    // Clean up block ---

    KStream<String, Metric> totalcover = builder.stream(STREAMING_TOTALCOVER_TOPIC,
        Consumed.with(stringSerde, metricSerde));
    ;
    KStream<String, Metric> precision = builder.stream(STREAMING_PRECISION_TOPIC,
        Consumed.with(stringSerde, metricSerde));
    KStream<String, Metric> bmrelevant = builder.stream(STREAMING_BMRELEVANT_TOPIC,
        Consumed.with(stringSerde, metricSerde));

    KStream<String, Metric> writingTime = builder.stream(STREAMING_WRITINGTIME_TOPIC,
        Consumed.with(stringSerde, metricSerde));

    KStream<String,Metric> pagestay=builder.stream(STREAMING_PAGE_STAY_TOPIC,Consumed.with(stringSerde,metricSerde));

    ProcessorSupplier<String, Metric> ttl_suplier = new ProcessorSupplier<String, Metric>() {
      public Processor<String, Metric> get() {
        return new TTLStoreProcessor<Metric>(Duration.ofHours(1), Duration.ofHours(5), (key, value) -> value.type);
      }
    };
    totalcover.merge(bmrelevant).merge(precision).merge(writingTime).merge(pagestay).process(ttl_suplier, STREAMING_STATE_TTL_STORE,
        STREAMING_STATE_TOTALCOVER_STORE, STREAMING_STATE_BMRELEVANT_STORE, STREAMING_STATE_PRECISION_STORE,
        STREAMING_STATE_WRITING_TIME_STORE, STREAMING_STATE_DEDUP_STORE, STREAMING_STATE_REFERENCE_TIME_STORE,
        STREAMING_STATE_PAGE_STAY_STORE,STREAMING_STATE_TOTAL_PAGE_STAY_STORE,
        STREAMING_STATE_LAST_KEYSTROKES_STORE);

    // foreach((key, value) -> System.out.print(value.username + "=>" + value.url +
    // "\n"));
    // KGroupedStream<String, VisitedLink> linksByUser = visitedLinks.groupBy((key,
    // value) -> value.username + value.url);

    // KTable<String, Long> pagesByUser = linksByUser.reduce((aggValue, newValue) ->
    // aggValue)
    // .groupBy((key, value) -> KeyValue.pair(value.username, value))
    // .count(Materialized.<String, Long, KeyValueStore<Bytes,
    // byte[]>>as("totalcover-store"));

    // KStream<String,TotalCoverMetric> totalcoverElements=
    // pagesByUser.toStream().map((key,value)-> KeyValue.pair(key,new
    // TotalCoverMetric(key,value)));
    // totalcoverElements.to("totalcover",
    // Produced.with(Serdes.String(),CustomSerders.TotalCoverMetric()));

    // KTable<Windowed<String>,Long>
    // linskByUserWithTimeWindow=linksByUser.reduce((aggValue, newValue) ->
    // aggValue).toStream()
    // .groupBy((key, value) ->value.username)
    // .windowedBy(TimeWindows.of(Duration.ofMinutes(1))).count(Materialized.<String,
    // Long, WindowStore<Bytes, byte[]>>as("totalcover-store-windowed"));

    // linskByUserWithTimeWindow.toStream().foreach((key, value) ->
    // System.out.print(key + "=>" + value + "\n"));
    Topology topology = builder.build();
    System.out.println(topology.describe());
    return new KafkaStreams(topology, streamsConfiguration);
  }

}