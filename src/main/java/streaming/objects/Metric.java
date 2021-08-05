package streaming.objects;


public class Metric {

    public String username;
    public Double value;
    public String type;

    public Metric(String username,Double value,String type){
        this.username=username;
        this.value=value;
        this.type=type;
    }


    public Metric() {

    }

    
}