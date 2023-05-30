package streaming.objects;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class VisitedLink {
    public String url;
    public String state;
    public Double localTimestamp;
    public String userId;

    @JsonProperty("payload")
    private void unpackNested(Map<String,Object> payload) {
        this.url=(String)payload.get("url");
        this.state= (String) payload.get("state");
        Double localTimestamp= (Double) payload.get("localTimeStamp");
        this.localTimestamp= localTimestamp;
        this.userId=(String) payload.get("userId");
        
    }
    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return "D11";
    }
}
