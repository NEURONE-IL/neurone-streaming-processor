package totalcover;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import totalcover.objects.Bookmark;
import totalcover.objects.TotalCoverMetric;
import totalcover.objects.VisitedLink;
import totalcover.utils.CustomSerders;
import totalcover.process.DeduplicationTransformer;

import java.time.Duration;
import java.util.Properties;

public class TotalCover {

  static final String TEST_VISITEDLINKS_TOPIC = "test.visitedlinks";
  static final String DB_BOOKMARKS_TOPIC = "test.bookmarks";
  static final String DEFAULT_HOST = "localhost";
  static final String VISITED_LINK_STORE = "visitedlinks-store";

  public static void main(final String[] args) throws Exception {
    if (args.length == 0 || args.length > 2) {
      throw new IllegalArgumentException("usage: ... <portForRestEndPoint> [<bootstrap.servers> (optional)]");
    }

    final int port = Integer.parseInt(args[0]);
    final String bootstrapServers = args.length > 1 ? args[1] : "localhost:9092";

    final Properties streamsConfiguration = new Properties();

    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "totalcover-application");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
    streamsConfiguration.put("default.deserialization.exception.handler", LogAndContinueExceptionHandler.class);
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,50);
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
    new TotalCoverRestService(streams, hostInfo);
  }

  public static KafkaStreams createStreams(final Properties streamsConfiguration) {

    final StreamsBuilder builder = new StreamsBuilder();

    // https://kafka-tutorials.confluent.io/finding-distinct-events/kstreams.html
    final StoreBuilder<KeyValueStore<String, Long>> dedupStoreBuilder = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore(VISITED_LINK_STORE), Serdes.String(), Serdes.Long());

    builder.addStateStore(dedupStoreBuilder);

    // foreach((key, value) -> System.out.print(key + "=>" + value.toString() +
    // "\n"));

    KStream<String, VisitedLink> visitedLinks = builder.stream(TEST_VISITEDLINKS_TOPIC,
        Consumed.with(Serdes.String(), CustomSerders.VisitedLink()));

    // visitedLinks.foreach((key, value) -> System.out.println(" => (" +
    // value.username+ ","+value.url+")\n"));
    ValueTransformerWithKeySupplier<String, VisitedLink, VisitedLink> suplier = new ValueTransformerWithKeySupplier<String, VisitedLink, VisitedLink>() {
      public ValueTransformerWithKey<String, VisitedLink, VisitedLink> get() {
        return new DeduplicationTransformer<String, VisitedLink, String>(VISITED_LINK_STORE,
            (key, value) -> value.username + value.url, "visitedlink");
      }

    };

    KTable<String, Long> totalCoverStore = visitedLinks.filter((key, value) -> value.url.contains("page"))
        .transformValues(suplier, VISITED_LINK_STORE).filter((k, v) -> v != null).groupByKey()
        .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("totalcover-store"));

    totalCoverStore.toStream().map((key, value) -> KeyValue.pair(key, new TotalCoverMetric(key, (double) value, "totalcover")))
        .to("totalcover", Produced.with(Serdes.String(), CustomSerders.TotalCoverMetric()));

    ValueTransformerWithKeySupplier<String, Bookmark, Bookmark> suplier_bookmark = new ValueTransformerWithKeySupplier<String, Bookmark, Bookmark>() {
      public ValueTransformerWithKey<String, Bookmark, Bookmark> get() {
        return new DeduplicationTransformer<String, Bookmark, String>(VISITED_LINK_STORE,
            (key, value) -> String.format("%s,%s,%s", value.username, value.url, value.action), "bookmark");
      }
    };

    KStream<String, Bookmark> bookmarks = builder.stream(DB_BOOKMARKS_TOPIC,
        Consumed.with(Serdes.String(), CustomSerders.Bookmark()));

    KTable<String, Long> bmRelevantStore = bookmarks.filter((key, value) -> value.relevant && value.userMade)
        .transformValues(suplier_bookmark, VISITED_LINK_STORE).filter((k, v) -> v != null)
        .groupByKey(Grouped.with(Serdes.String(), CustomSerders.Bookmark()))
        .aggregate(() -> 0L, (aggKey, newValue, aggValue) -> {

          if (newValue.action.equals("Bookmark")) {
            return aggValue + 1;
          } else {
            return aggValue - 1;
          }
          // return aggValue + (newValue.action.equals("Bookmark")? 1L:-1L);

        }, Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("bmrelevant-store")
            .withValueSerde(Serdes.Long()));

    bmRelevantStore.toStream().map((key, value) -> KeyValue.pair(key, new TotalCoverMetric(key, (double) value, "bmrelevant")))
        .to("bmrelevant", Produced.with(Serdes.String(), CustomSerders.TotalCoverMetric()));


    KTable<String,Double> precision= totalCoverStore.join(bmRelevantStore, (totalcover,bmrelevant)-> {
      
      if((double) bmrelevant==0.0 || (double) totalcover==0.0){
        return 0.0;
      } else{
          return (double) bmrelevant/(double) totalcover;
      }},
    Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as("precision-store")
            .withValueSerde(Serdes.Double()) );

    precision.toStream().map((key, value) -> KeyValue.pair(key, new TotalCoverMetric(key, value, "precision")))
    .to("precision", Produced.with(Serdes.String(), CustomSerders.TotalCoverMetric()));
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

    return new KafkaStreams(builder.build(), streamsConfiguration);
  }

}