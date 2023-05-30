package streaming.objects;


public class Metric {

    public String userId;
    public Double value;
    public String type;

    public Metric(String userId,Double value,String type){
        this.userId=userId;
        this.value=value;
        this.type=type;
    }


    public Metric() {

    }

    
}