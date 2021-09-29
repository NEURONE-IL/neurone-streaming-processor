package streaming.objects;

public class KafkaMetric {
    

    public String name;
    public String operator;
    public Double value;
    public String group;

    public KafkaMetric(String name,String operator, String group, Double value){
        this.name=name;
        this.operator=operator;
        this.value=value;
        this.group=group;
    }
    public KafkaMetric(){

    }
}
