package streaming.process;

import java.time.Duration;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.kstream.KeyValueMapper;

public class TTLStoreProcessor<V> implements Processor<String, V> {

    private final Duration maxAge;
    private final Duration scanFrequency;
    private final String ttlStoreName;
    private ProcessorContext context;

    private KeyValueStore<String, Long> ttlStore;
    private KeyValueStore<String, Long> totalCoverStore;
    private KeyValueStore<String, Long> bmRelevantStore;
    private KeyValueStore<String, Long> precisionStore;
    private KeyValueStore<String, Long> dedupStore;

    private final KeyValueMapper<String, V,String> storeEtractor;

    public TTLStoreProcessor(final Duration maxAge, 
    final Duration scanFrequency, final String ttlStoreName,
    KeyValueMapper<String, V,String> storeEtractor) {
        this.maxAge = maxAge;
        this.scanFrequency = scanFrequency;
        this.ttlStoreName = ttlStoreName;
        this.storeEtractor=storeEtractor;

    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.ttlStore = context.getStateStore(ttlStoreName);
        this.totalCoverStore = context.getStateStore("totalcover-store");
        this.bmRelevantStore = context.getStateStore("bmrelevant-store");
        this.precisionStore = context.getStateStore("precision-store");
        this.dedupStore = context.getStateStore("dedup-store");
        context.schedule(scanFrequency, PunctuationType.WALL_CLOCK_TIME, timestamp -> {

            final long cutoff = timestamp - maxAge.toMillis();

            try (final KeyValueIterator<String, Long> all = ttlStore.all()) {

                while (all.hasNext()) {
                    final KeyValue<String, Long> record = all.next();
                    if (record.value != null && record.value < cutoff) {
                        
                        String parsedId = (String) record.key;
                        String[] keyComponents = parsedId.split("-");
                        String storeName = keyComponents[1];
                        String recordKey = keyComponents[0];
                        System.out.println("Deleteing "+recordKey +"from "+storeName);
                        this.deleteRecors(storeName, recordKey);
                        this.ttlStore.delete(parsedId);

                    }
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        });
    }

    private void deleteRecors(String storeName, String key) {

        switch (storeName) {
            case "totalcover":
                this.totalCoverStore.delete(key);
                break;
            case "bmrelevant":
                this.bmRelevantStore.delete(key);
                break;
            case "precision":
                this.precisionStore.delete(key);
                break;
            case "dedup":
                this.dedupStore.delete(key);
                break;
            default:
                break;
        }

    
    }

    @Override
    public void process(String key, V value) {

        final String type= storeEtractor.apply(key, value);
        this.ttlStore.put(key+"-"+type, context.timestamp());

    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

}
