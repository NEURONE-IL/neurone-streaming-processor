package streaming.process;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
public class ReferenceTimeProcessor implements Processor<String,Long> {

    

    private KeyValueStore<String,ArrayList<Long>> referenceStore;
    private final String referenceStoreName;
    

    public ReferenceTimeProcessor(final String referenceStoreName){
        this.referenceStoreName=referenceStoreName;
    }
    @Override
    public void init(ProcessorContext context) {
       
        this.referenceStore=context.getStateStore(referenceStoreName);
        
    }

    @Override
    public void process(String key, Long value) {
    
        ArrayList<Long> referenceList=this.referenceStore.get(key);
        
        if (referenceList==null){
            referenceList= new ArrayList<Long>();
        } 
        referenceList.add(value);
        this.referenceStore.put(key, referenceList);
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        
    }
    
    
}
