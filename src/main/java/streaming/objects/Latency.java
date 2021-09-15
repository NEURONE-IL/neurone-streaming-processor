package streaming.objects;

public class Latency {
    

    public String name;
    public String operator;
    public Double value;

    public Latency(String name,String operator, Double value){
        this.name=name;
        this.operator=operator;
        this.value=value;
    }
    public Latency(){

    }
}
