package streaming.process;

import java.time.Duration;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import streaming.objects.VisitedLink;

import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.processor.PunctuationType;
import static streaming.utils.AppProperties.STREAMING_FIRST_PAGE_STORE;

public class FirstQueryTimeTransformer implements Transformer<String,VisitedLink,KeyValue<String,Double>>{

    private KeyValueStore<String, Long> firstPageStore;

    private final Duration scanFrequency;

    private ProcessorContext context;

    public FirstQueryTimeTransformer( final Duration scanFrequency) {

        this.scanFrequency = scanFrequency;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.firstPageStore = this.context.getStateStore(STREAMING_FIRST_PAGE_STORE);

        context.schedule(scanFrequency, PunctuationType.WALL_CLOCK_TIME, timestamp -> {

            try (final KeyValueIterator<String, Long> all = firstPageStore.all()) {

                while (all.hasNext()) {
                    final KeyValue<String, Long> record = all.next();

                    Double diff = (double) timestamp - record.value;
                    diff = diff / 1000;

                    context.forward(record.key, diff);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

    }

    @Override
    public KeyValue<String,Double> transform(String key, VisitedLink value) {

        if (value.url.equals("/session/search")) {

            this.firstPageStore.put(key, value.localTimestamp.longValue());
        } else if (this.firstPageStore.get(key) != null) {
            this.firstPageStore.delete(key);
        }
        return null;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

}
