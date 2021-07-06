package totalcover;

import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.common.KafkaFuture.Function;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// import org.glassfish.jersey.jackson.JacksonFeature;

import totalcover.utils.JsonTransformer;

// import org.glassfish.jersey.server.ResourceConfig;
// import org.glassfish.jersey.servlet.ServletContainer;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.GenericType;

import com.fasterxml.jackson.databind.ObjectMapper;

import static spark.Spark.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class TotalCoverRestService {

 
  // private final MetadataService metadataService;

  public TotalCoverRestService(final KafkaStreams streams, final HostInfo hostInfo){

    port(hostInfo.port());
    get("/keyvalue/:storeName/:key", (req, res) -> {

      String storeName= req.params("storeName");
      String key=req.params("key");

      final ReadOnlyKeyValueStore<String, Long> store = streams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));

      final Long value= store.get(key);
      if (value ==null){
        notFound((request,response)->{
          return "{\"message\":\"404\"}";
        
        });
      }
      
      return new KeyValueBean(key,value);
    }, new JsonTransformer());

    get("/keyvalue/:storeName",(req,res)->{

      try {
        
        String storeName= req.params("storeName");
        final ReadOnlyKeyValueStore<String, Object> store = streams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
  
        KeyValueIterator<String,Object> range= store.all();
        final List<KeyValueBean> results = new ArrayList<>();
        while (range.hasNext()) {
          final KeyValue<String, Object> next = range.next();
          results.add(new KeyValueBean(next.key, next.value));
        }
        range.close();
        return results;
      } catch (Exception e) {
        e.printStackTrace();
        return null;
        //TODO: handle exception
      }
    },new JsonTransformer());


    get("/keyvalue/:storeName/:key/:from/:to",(req,res)->{

      String storeName= req.params("storeName");
      String key=req.params("key");
      Long from=Long.parseLong(req.params("from"));
      Long to=Long.parseLong( req.params("to"));
      final ReadOnlyWindowStore<String, Long> store = streams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.windowStore()));

      if (store == null) {
        throw new NotFoundException();
      }
   // fetch the window results for the given key and time range
      final WindowStoreIterator<Long> results = store.fetch(key, Instant.ofEpochMilli(from), Instant.ofEpochMilli(to));
      final List<KeyValueBean> windowResults = new ArrayList<>();
      while (results.hasNext()) {
        final KeyValue<Long, Long> next = results.next();
        // convert the result to have the window time and the key (for display purposes)
        windowResults.add(new KeyValueBean(key + "@" + next.key, next.value));
      }
      return windowResults;

    },new JsonTransformer());


    get("/range/:storeName/:from/:to",(req,res)->{
      String storeName= req.params("storeName");
      Long from=Long.parseLong(req.params("from"));
      Long to=Long.parseLong( req.params("to"));

      final ReadOnlyWindowStore<String, Long> store = streams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.windowStore()));

      if (store == null) {
        System.out.println("NOT FOUND");
        throw new NotFoundException();
      }
   // fetch the window results for the given key and time range
   System.out.println("FETCHING RESULTS");
      final KeyValueIterator<Windowed<String>,Long> results = store.fetchAll(Instant.ofEpochMilli(from), Instant.ofEpochMilli(to));
      final List<KeyValueBean> windowResults = new ArrayList<>();
      while (results.hasNext()) {
        System.out.println("ITERATIOND");
        final KeyValue<Windowed<String>,Long> next = results.next();
        
        // convert the result to have the window time and the key (for display purposes)
        windowResults.add(new KeyValueBean(next.key.key() + "@" + next.key , next.value));
      }
      System.out.println("RETURN");
      return windowResults;

    },new JsonTransformer());


    after((req,res)->{

      res.type("application/json");
    });
  }

}
