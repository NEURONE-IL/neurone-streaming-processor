package streaming.process;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;

import streaming.objects.MetricValue;
import streaming.objects.Metadata;

public class MetricValueTransformer<K, V, E> implements ValueTransformerWithKey<String, V, MetricValue> {

    private final String storeName;
    private KeyValueStore<String, Metadata> metadataStore;
    private ProcessorContext context;

    public MetricValueTransformer(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.metadataStore = this.context.getStateStore(storeName);
    }

    @Override
    public MetricValue transform(String key, V value) {
        Metadata metadata = metadataStore.get((String) key);
        MetricValue metricValue = new MetricValue(metadata, value);
        return metricValue;
    }

    @Override
    public void close() {
        // clean up code
    }
}