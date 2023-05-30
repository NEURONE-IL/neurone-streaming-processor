package streaming.objects;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Query {
    public String url;
    public Double localTimestamp;
    public String query;
    public String userId;

    @JsonProperty("payload")
    private void unpackNested(Map<String,Object> payload){
        this.url=(String)payload.get("url");
        Double localTimestamp= (Double) payload.get("localTimeStamp");
        this.localTimestamp= localTimestamp;
        this.query= (String) payload.get("query");
        this.userId=(String) payload.get("userId");
    }
}
