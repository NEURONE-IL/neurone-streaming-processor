package streaming.process;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import streaming.objects.VisitedLink;

import static streaming.utils.AppProperties.STREAMING_STATE_PAGE_STAY_STORE;

import javax.naming.Context;

import static streaming.utils.AppProperties.STREAMING_STATE_PAGE_SEQUENCE_STORE;


public class PageStayTransformer implements ValueTransformerWithKey<String,VisitedLink,Double> {

    private KeyValueStore<String,Double> pageStayStore;
    private KeyValueStore<String,Long> pageSequenceStore;
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context=context;
        this.pageStayStore=context.getStateStore(STREAMING_STATE_PAGE_STAY_STORE);
        this.pageSequenceStore=context.getStateStore(STREAMING_STATE_PAGE_SEQUENCE_STORE);
    }

    @Override
    public Double transform(String key, VisitedLink value) {
        
        String id=key+"-"+value.url;
        Double result=0D;

        Long lastTimestamp= this.pageSequenceStore.get(id);
        if(lastTimestamp==null && value.state.equals("PageEnter")){
            
            
            this.pageSequenceStore.put(id,value.localTimestamp.longValue());
           
        } else if( lastTimestamp!=null && value.state.equals("PageExit")){

            this.pageSequenceStore.delete(id);
            result=value.localTimestamp-lastTimestamp;
            this.pageStayStore.put(key, result);
            
        }
        return result;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        
    }
    
}
