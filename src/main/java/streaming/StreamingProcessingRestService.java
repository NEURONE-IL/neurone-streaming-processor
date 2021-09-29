package streaming;


import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
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

// import org.glassfish.jersey.jackson.JacksonFeature;

import streaming.objects.KafkaMetric;
import streaming.utils.JsonTransformer;

// import org.glassfish.jersey.server.ResourceConfig;
// import org.glassfish.jersey.servlet.ServletContainer;


import javax.ws.rs.NotFoundException;


import static spark.Spark.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamingProcessingRestService {


  public StreamingProcessingRestService(final KafkaStreams streams, final HostInfo hostInfo){

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

    get("/metrics",(res,req)->{

      try {
        

        HashMap<String,Object> response=new HashMap<>();
        HashMap<MetricName,Metric> metrics= new HashMap<MetricName,Metric>(streams.metrics());
        List<KafkaMetric> metricsList= new ArrayList<>();
     
        for(Map.Entry<MetricName,Metric> entry: metrics.entrySet()){
      

          if((entry.getKey().name().contains("record-e2e-latency") && entry.getKey().tags().containsKey("processor-node-id") 
          && entry.getKey().tags().get("processor-node-id").contains("sink")) || 
          entry.getKey().name().contains("process-rate") || entry.getKey().name().contains("process-total") 
          ){

            // System.out.println(entry.getKey());
            // System.out.println(entry.getValue().metricValue());
            metricsList.add(new KafkaMetric(entry.getKey().name(),
            entry.getKey().tags().get("processor-node-id"),
            entry.getKey().group(),
            (Double) entry.getValue().metricValue()));
          }
        }
        response.put("timestamp", System.currentTimeMillis());
        response.put("metrics",metricsList);
        return response;
      } catch (Exception e) {
        e.printStackTrace();
        return null;
      }
    },new JsonTransformer());

    after((req,res)->{

      res.type("application/json");
    });
  }

}
