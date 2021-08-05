package streaming.process;

import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class DeduplicationTransformer<K, V, E> implements ValueTransformerWithKey<K, V, V> {

    private KeyValueStore<E, Long> dedupStateStore;
    private final String storeName;
    private final String ttlStoreName;
    private ProcessorContext context;
    private final KeyValueMapper<K, V, E> idExtractor;
    private  KeyValueStore<String,Long> ttlStore;
    private final String type;

    public DeduplicationTransformer(String storeName, String ttlStoreName, 
    final KeyValueMapper<K, V, E> idExtractor, String type) {
        this.storeName = storeName;
        this.ttlStoreName=ttlStoreName;
        this.idExtractor = idExtractor;
        this.type = type;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.dedupStateStore = this.context.getStateStore(storeName);
        this.ttlStore=this.context.getStateStore(ttlStoreName);

    }

    @Override
    public V transform(final K key, final V value) {
        final E id = idExtractor.apply(key, value);

        if (id == null) {

            return value;
        } else {
            final V output;

            boolean checkRecordValue;

            switch (this.type) {
                case "visitedlink":
                    checkRecordValue = isDuplicate(id);
                    break;
                case "bookmark":
                    checkRecordValue = isDuplicateBookmark(id);
                    break;
                default:
                    checkRecordValue = false;
                    break;
            }
            if (checkRecordValue) {
                output = null;
            } else {
                output = value;
                
            }
            return output;
        }
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }
    public Boolean isDuplicate(E id) {

        if (dedupStateStore.get(id) != null) {
            return true;
        } else {
            dedupStateStore.put(id, context.timestamp());
            ttlStore.put(id+"-dedup",context.timestamp());
            return false;
        }
    }

    public Boolean isDuplicateBookmark(E id) {
        if (dedupStateStore.get(id) != null) {
            return true;
        } else {
            String parsedId= (String) id;
            String action= getAction(parsedId);
            String complement=getComplement(parsedId);
            E complementParsed= (E) complement;
            if (action.equals("Unbookmark") && dedupStateStore.get(complementParsed)!=null){
                dedupStateStore.delete(complementParsed);
                return false;
            } else if(action.equals("Bookmark")){
                dedupStateStore.put(id, context.timestamp());
                ttlStore.put(id+"-dedup",context.timestamp());
                return false;
            } else{
                return true;
            }
        }
    }

    public String getAction(String id){
        String action= id.split(",")[2];
        return action;
    }

    public String getComplement(String id){

        String[] idElements=id.split(",");

        return String.format("%s,%s,%s",idElements[0],idElements[1],idElements[2].equals("Bookmark")? "Unbookmark":"Bookmark");
    }

}
