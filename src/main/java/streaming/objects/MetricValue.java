package streaming.objects;

public class MetricValue {
    public Metadata metadata;
    public Object value;

    public MetricValue(Metadata metadata, Object value) {
        this.metadata = metadata;
        this.value = value;
    }
}
