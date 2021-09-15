package streaming;

import org.apache.kafka.common.serialization.Serdes;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

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
import streaming.objects.Metric;
import streaming.objects.VisitedLink;
import streaming.process.DeduplicationTransformer;
import streaming.process.TTLStoreProcessor;
import streaming.utils.CustomSerders;

import java.time.Duration;
import java.util.Properties;

public class StreamingProcessing {

  static final String TEST_VISITEDLINKS_TOPIC = "neurone.visitedlinks";
  static final String DB_BOOKMARKS_TOPIC = "neurone.bookmarks";
  static final String DEFAULT_HOST = "localhost";
  static final String DEDUP_STORE = "dedup-store";
  static final String TTL_STORE = "ttl-store";

  public static void main(final String[] args) throws Exception {
    if (args.length == 0 || args.length > 2) {
      throw new IllegalArgumentException("usage: ... <portForRestEndPoint> [<bootstrap.servers> (optional)]");
    }

    final int port = Integer.parseInt(args[0]);
    final String bootstrapServers = args.length > 1 ? args[1] : "localhost:9092";

    final Properties streamsConfiguration = new Properties();

    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "totalcover-application-1");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
    // streamsConfiguration.put("default.deserialization.exception.handler",
    // LogAndContinueExceptionHandler.class);
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 50);
    streamsConfiguration.put(StreamsConfig.APPLICATION_SERVER_CONFIG, DEFAULT_HOST + ":" + port);

    final KafkaStreams streams = createStreams(streamsConfiguration);

    streams.cleanUp();

    streams.start();

    startRestProxy(streams, DEFAULT_HOST, port);

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

    // https://kafka-tutorials.confluent.io/finding-distinct-events/kstreams.html
    final StoreBuilder<KeyValueStore<String, Long>> dedupStoreBuilder = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore(DEDUP_STORE), Serdes.String(), Serdes.Long());

    final StoreBuilder<KeyValueStore<String, Long>> ttlStoreBuilder = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore(TTL_STORE), Serdes.String(), Serdes.Long());
    builder.addStateStore(dedupStoreBuilder);
    builder.addStateStore(ttlStoreBuilder);

    // foreach((key, value) -> System.out.print(key + "=>" + value.toString() +
    // "\n"));

    KStream<String, VisitedLink> visitedLinks = builder.stream(TEST_VISITEDLINKS_TOPIC,
        Consumed.with(Serdes.String(), CustomSerders.VisitedLink()).withName("visitedlinks_input_topic"));

    // visitedLinks.foreach((key, value) -> System.out.println(" => (" +
    // value.username+ ","+value.url+")\n"));
    ValueTransformerWithKeySupplier<String, VisitedLink, VisitedLink> suplier = new ValueTransformerWithKeySupplier<String, VisitedLink, VisitedLink>() {
      public ValueTransformerWithKey<String, VisitedLink, VisitedLink> get() {
        return new DeduplicationTransformer<String, VisitedLink, String>(DEDUP_STORE, TTL_STORE,
            (key, value) -> value.username + value.url, "visitedlink");
      }

    };

    KTable<String, Long> totalCoverStore = visitedLinks
        .filter((key, value) -> value.url.contains("page"), Named.as("filter_page_links"))
        .transformValues(suplier, DEDUP_STORE, TTL_STORE).filter((k, v) -> v != null, Named.as("filter_not_null_links"))
        .groupByKey().count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("totalcover-store"));

    totalCoverStore.toStream().filter((key, value) -> key != null && value != null, Named.as("filter_null_totalcover"))
        .map((key, value) -> KeyValue.pair(key, new Metric(key, (double) value, "totalcover")),
            Named.as("build_metric_totalcover"))
        .to("totalcover",
            Produced.with(Serdes.String(), CustomSerders.TotalCoverMetric()).withName("sink_totalcover_topic"));

    ValueTransformerWithKeySupplier<String, Bookmark, Bookmark> suplier_bookmark = new ValueTransformerWithKeySupplier<String, Bookmark, Bookmark>() {
      public ValueTransformerWithKey<String, Bookmark, Bookmark> get() {
        return new DeduplicationTransformer<String, Bookmark, String>(DEDUP_STORE, TTL_STORE,
            (key, value) -> String.format("%s,%s,%s", value.username, value.url, value.action), "bookmark");
      }
    };

    KStream<String, Bookmark> bookmarks = builder.stream(DB_BOOKMARKS_TOPIC,
        Consumed.with(Serdes.String(), CustomSerders.Bookmark()).withName("bookmark_input_topic"));

    KTable<String, Long> bmRelevantStore = bookmarks.filter((key, value) -> value.relevant && value.userMade)
        .transformValues(suplier_bookmark, DEDUP_STORE, TTL_STORE)
        .filter((k, v) -> v != null, Named.as("filter_not_null_boorkmarks"))
        .groupByKey(Grouped.with(Serdes.String(), CustomSerders.Bookmark()).withName("group_bookmarks"))
        .aggregate(() -> 0L, (aggKey, newValue, aggValue) -> {

          if (newValue.action.equals("Bookmark")) {
            return aggValue + 1;
          } else {
            return aggValue - 1;
          }
          // return aggValue + (newValue.action.equals("Bookmark")? 1L:-1L);

        }, Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("bmrelevant-store")
            .withValueSerde(Serdes.Long()));

    bmRelevantStore.toStream()
        .filter((key, value) -> key != null && value != null, Named.as("Filter_not_null_bmrelevant"))
        .map((key, value) -> KeyValue.pair(key, new Metric(key, (double) value, "bmrelevant")),
            Named.as("build_bmrelevant"))
        .to("bmrelevant",
            Produced.with(Serdes.String(), CustomSerders.TotalCoverMetric()).withName("sink_bmrelevant_topic"));

    KTable<String, Double> precisionStore = totalCoverStore.join(bmRelevantStore, (totalcover, bmrelevant) -> {

      if ((double) bmrelevant == 0.0 || (double) totalcover == 0.0) {
        return 0.0;
      } else {
        return (double) bmrelevant / (double) totalcover;
      }
    }, Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as("precision-store")
        .withValueSerde(Serdes.Double()));

    precisionStore.toStream().filter((key, value) -> key != null && value != null, Named.as("filter_not_nul_precision"))
        .map((key, value) -> KeyValue.pair(key, new Metric(key, value, "precision")),
            Named.as("build_precision_metric"))
        .to("precision",
            Produced.with(Serdes.String(), CustomSerders.TotalCoverMetric()).withName("sink_precision_topic"));

    // Clean up block
    KStream<String, Metric> totalcover = builder.stream("totalcover",
        Consumed.with(Serdes.String(), CustomSerders.TotalCoverMetric()));
    ;
    KStream<String, Metric> precision = builder.stream("precision",
        Consumed.with(Serdes.String(), CustomSerders.TotalCoverMetric()));
    KStream<String, Metric> bmrelevant = builder.stream("bmrelevant",
        Consumed.with(Serdes.String(), CustomSerders.TotalCoverMetric()));

    ProcessorSupplier<String, Metric> ttl_suplier = new ProcessorSupplier<String, Metric>() {
      public Processor<String, Metric> get() {
        return new TTLStoreProcessor<Metric>(Duration.ofHours(1), Duration.ofHours(5), TTL_STORE,
            (key, value) -> value.type);
      }
    };
    totalcover.merge(bmrelevant).merge(precision).process(ttl_suplier, TTL_STORE, "totalcover-store",
        "bmrelevant-store", "precision-store", DEDUP_STORE);

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