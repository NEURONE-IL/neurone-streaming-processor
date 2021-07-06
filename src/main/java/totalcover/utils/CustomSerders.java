//https://medium.com/@agvillamizar/implementing-custom-serdes-for-java-objects-using-json-serializer-and-deserializer-in-kafka-streams-d794b66e7c03
package totalcover.utils;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;


import totalcover.objects.Bookmark;
import totalcover.objects.TotalCoverMetric;
import totalcover.objects.VisitedLink;

public final class CustomSerders {

    static public final class VisitedLinkSerde extends Serdes.WrapperSerde<VisitedLink> {

        public VisitedLinkSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(VisitedLink.class));
        }
    }

    static public final class TotalCoverMetricSerde extends Serdes.WrapperSerde<TotalCoverMetric>{
        public  TotalCoverMetricSerde(){
            super(new JsonSerializer<>(),new JsonDeserializer<>(TotalCoverMetric.class));
        }
    }

    static public final class BookmarkSerde extends Serdes.WrapperSerde<Bookmark>{
        public BookmarkSerde(){
            super(new JsonSerializer<>(), new JsonDeserializer<>(Bookmark.class));
        }
    }

    public static Serde<VisitedLink> VisitedLink() {
        return new CustomSerders.VisitedLinkSerde();
    }

    public static Serde<TotalCoverMetric> TotalCoverMetric(){
        return new CustomSerders.TotalCoverMetricSerde();
    }

    public static Serde<Bookmark> Bookmark(){
        return new CustomSerders.BookmarkSerde();
    }

}