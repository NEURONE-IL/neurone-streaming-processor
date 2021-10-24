//https://medium.com/@agvillamizar/implementing-custom-serdes-for-java-objects-using-json-serializer-and-deserializer-in-kafka-streams-d794b66e7c03
package streaming.utils;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import streaming.objects.Bookmark;
import streaming.objects.KeyStroke;
import streaming.objects.Metric;
import streaming.objects.Query;
import streaming.objects.VisitedLink;

public final class CustomSerders {

    static public final class VisitedLinkSerde extends Serdes.WrapperSerde<VisitedLink> {

        public VisitedLinkSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(VisitedLink.class));
        }
    }

    static public final class MetricSerde extends Serdes.WrapperSerde<Metric>{
        public  MetricSerde(){
            super(new JsonSerializer<>(),new JsonDeserializer<>(Metric.class));
        }
    }

    static public final class BookmarkSerde extends Serdes.WrapperSerde<Bookmark>{
        public BookmarkSerde(){
            super(new JsonSerializer<>(), new JsonDeserializer<>(Bookmark.class));
        }
    }

    static public final class KeystrokeSerde extends Serdes.WrapperSerde<KeyStroke>{
        public KeystrokeSerde(){
            super(new JsonSerializer<>(), new JsonDeserializer<>(KeyStroke.class));
        }
    }
    static public final class QuerySerde extends Serdes.WrapperSerde<Query>{
        public QuerySerde(){
            super(new JsonSerializer<>(), new JsonDeserializer<>(Query.class));
        }
    }



    public static Serde<VisitedLink> VisitedLink() {
        return new CustomSerders.VisitedLinkSerde();
    }

    public static Serde<Metric> Metric(){
        return new CustomSerders.MetricSerde();
    }

    public static Serde<Bookmark> Bookmark(){
        return new CustomSerders.BookmarkSerde();
    }

    public static Serde<KeyStroke> Keystrokes(){
        return new CustomSerders.KeystrokeSerde();
    }

    public static Serde<Query> Query(){
        return new CustomSerders.QuerySerde();
    }
}