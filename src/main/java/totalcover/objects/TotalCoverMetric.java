package totalcover.objects;


public class TotalCoverMetric {

    public String username;
    public Double value;
    public String type;

    public TotalCoverMetric(String username,Double value,String type){
        this.username=username;
        this.value=value;
        this.type=type;
    }
}