package streaming.process;

import java.time.Duration;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.ArrayList;

import static streaming.utils.AppProperties.STREAMING_STATE_WRITING_TIME_STORE;
import static streaming.utils.AppProperties.STREAMING_STATE_TOTALCOVER_STORE;
import static streaming.utils.AppProperties.STREAMING_STATE_BMRELEVANT_STORE;
import static streaming.utils.AppProperties.STREAMING_STATE_PRECISION_STORE;
import static streaming.utils.AppProperties.STREAMING_STATE_DEDUP_STORE;
import static streaming.utils.AppProperties.STREAMING_STATE_TTL_STORE;
import static streaming.utils.AppProperties.STREAMING_STATE_REFERENCE_TIME_STORE;
import static streaming.utils.AppProperties.STREAMING_STATE_LAST_KEYSTROKES_STORE;
import static streaming.utils.AppProperties.STREAMING_STATE_PAGE_STAY_STORE;
import static streaming.utils.AppProperties.STREAMING_STATE_TOTAL_PAGE_STAY_STORE;

import static streaming.utils.AppProperties.STREAMING_TOTALCOVER_TOPIC;
import static streaming.utils.AppProperties.STREAMING_BMRELEVANT_TOPIC;
import static streaming.utils.AppProperties.STREAMING_PRECISION_TOPIC;
import static streaming.utils.AppProperties.STREAMING_WRITINGTIME_TOPIC;
import static streaming.utils.AppProperties.STREAMING_PAGE_STAY_TOPIC;

public class TTLStoreProcessor<V> implements Processor<String, V> {

    private final Duration maxAge;
    private final Duration scanFrequency;

    private ProcessorContext context;

    private KeyValueStore<String, Long> ttlStore;
    private KeyValueStore<String, Long> totalCoverStore;
    private KeyValueStore<String, Long> bmRelevantStore;
    private KeyValueStore<String, Long> precisionStore;
    private KeyValueStore<String, Double> writingTimeStore;
    private KeyValueStore<String, Long> dedupStore;
    private KeyValueStore<String, ArrayList<Long>> referenceStore;
    private KeyValueStore<String, Long> lastKeystrokeStore;
    private KeyValueStore<String, Double> pageStayStore;
    private KeyValueStore<String,Double> totalPageStayStore;

    private final KeyValueMapper<String, V, String> storeEtractor;

    public TTLStoreProcessor(final Duration maxAge, final Duration scanFrequency,
            KeyValueMapper<String, V, String> storeEtractor) {
        this.maxAge = maxAge;
        this.scanFrequency = scanFrequency;
        this.storeEtractor = storeEtractor;

    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;

        this.ttlStore = context.getStateStore(STREAMING_STATE_TTL_STORE);
        this.totalCoverStore = context.getStateStore(STREAMING_STATE_TOTALCOVER_STORE);
        this.bmRelevantStore = context.getStateStore(STREAMING_STATE_BMRELEVANT_STORE);
        this.precisionStore = context.getStateStore(STREAMING_STATE_PRECISION_STORE);
        this.dedupStore = context.getStateStore(STREAMING_STATE_DEDUP_STORE);
        this.writingTimeStore = context.getStateStore(STREAMING_STATE_WRITING_TIME_STORE);
        this.lastKeystrokeStore=context.getStateStore(STREAMING_STATE_LAST_KEYSTROKES_STORE);
        this.referenceStore=context.getStateStore(STREAMING_STATE_REFERENCE_TIME_STORE);
        this.pageStayStore=context.getStateStore(STREAMING_STATE_PAGE_STAY_STORE);
        this.totalPageStayStore=context.getStateStore(STREAMING_STATE_TOTAL_PAGE_STAY_STORE);

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
                        System.out.println("Deleteing " + recordKey + "from " + storeName);
                        this.deleteRecors(storeName, recordKey);
                        this.ttlStore.delete(parsedId);

                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private void deleteRecors(String storeName, String key) {

        if (storeName.equals(STREAMING_TOTALCOVER_TOPIC)) {
            this.totalCoverStore.delete(key);
        } else if (storeName.equals(STREAMING_BMRELEVANT_TOPIC)) {
            this.bmRelevantStore.delete(key);
        } else if (storeName.equals(STREAMING_PRECISION_TOPIC)) {
            this.precisionStore.delete(key);
        } else if (storeName.equals(STREAMING_WRITINGTIME_TOPIC)) {
            this.writingTimeStore.delete(key);
            this.referenceStore.delete(key);
            this.lastKeystrokeStore.delete(key);

        } else if(storeName.equals(STREAMING_PAGE_STAY_TOPIC)){
            this.pageStayStore.delete(key);
            this.totalPageStayStore.delete(key);
        }
         else if (storeName.equals("dedup")) {
            this.dedupStore.delete(key);
        }

    }

    @Override
    public void process(String key, V value) {

        final String type = storeEtractor.apply(key, value);
        this.ttlStore.put(key + "-" + type, context.timestamp());

    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

}
