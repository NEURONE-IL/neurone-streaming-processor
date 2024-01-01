package streaming.objects;


public class Metric {

    public String userId;
    public Double value;
    public String type;
    public Metadata metadata;

    public Metric(String userId,Double value,String type, Metadata metadata){
        this.userId=userId;
        this.value=value;
        this.type=type;
        this.metadata=metadata;
    }

    public Metric(String key, Object value, String topic, Metadata metadata) {
        this.userId = key;
        this.type = topic;
        this.metadata = metadata;

        if (value instanceof Long) {
            this.value = ((Long) value).doubleValue();
        } else if (value instanceof Double) {
            this.value = (Double) value;
        } else {
            throw new IllegalArgumentException("Unsupported type for value: " + value.getClass());
        }
    }

    public Metric() {

    }

    
}