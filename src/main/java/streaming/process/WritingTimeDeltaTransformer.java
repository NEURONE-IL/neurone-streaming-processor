package streaming.process;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import streaming.objects.KeyStroke;
import static streaming.utils.AppProperties.STREAMING_STATE_REFERENCE_TIME_STORE;
import static streaming.utils.AppProperties.STREAMING_STATE_LAST_KEYSTROKES_STORE;
import java.util.ArrayList;

public class WritingTimeDeltaTransformer<Keystrokes> implements ValueTransformerWithKey<String, KeyStroke, Double> {

    private KeyValueStore<String, ArrayList<Long>> referenceStore;
    private KeyValueStore<String, Long> lastKeystrokeStore;



   

    @Override
    public void init(ProcessorContext context) {
        this.lastKeystrokeStore = context.getStateStore(STREAMING_STATE_LAST_KEYSTROKES_STORE);
        this.referenceStore = context.getStateStore(STREAMING_STATE_REFERENCE_TIME_STORE);
    }

    @Override
    public Double transform(String key, KeyStroke value) {

        ArrayList<Long> referenceList = referenceStore.get(key);
        long referenceTimestamp = chooseReference(value.localTimestamp.longValue(), referenceList);
        Long lastTimestamp = lastKeystrokeStore.get(key);
        Double cutoff = (double) 0.0;
        lastKeystrokeStore.put(key, value.localTimestamp.longValue());

        if (lastTimestamp != null && (lastTimestamp > referenceTimestamp || referenceTimestamp == 0)) {
            cutoff = (double) value.localTimestamp - lastTimestamp;
        }

        return cutoff;
    }

    private long chooseReference(long currentTimestamp, ArrayList<Long> referenceList) {

        long ref = 0;
        if (referenceList == null) {
            return ref;
        }
        for (Long timestamp : referenceList) {
            if (currentTimestamp > timestamp) {
                ref = timestamp;
            }
        }
        return ref;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

}
