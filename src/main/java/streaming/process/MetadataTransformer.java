package streaming.process;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import streaming.interfaces.MetadataProvider;
import streaming.objects.Metadata;

public class MetadataTransformer<K, V extends MetadataProvider, E extends MetadataProvider>
        implements ValueTransformerWithKey<K, V, V> {

    private ProcessorContext context;
    private final String storeName;
    private KeyValueStore<K, Metadata> metadataStore;

    public MetadataTransformer(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.metadataStore = this.context.getStateStore(storeName);
    }

    @Override
    public V transform(K key, V value) {

        if (this.metadataStore.get(key) == null) {
            Metadata metadata = value.getMetadata();
            this.metadataStore.put(key, metadata);
        }
        return value;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }
}