package streaming.objects;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Event {
    public String url;
    public String type;
    public Double localTimestamp;
    public String userId;
    public String source;


    @JsonProperty("payload")
    private void unpackNested(Map<String,Object> payload) {
        this.url = (String)payload.get("url");
        this.type = (String) payload.get("type");
        Double localTimestamp= (Double) payload.get("localTimeStamp");
        this.localTimestamp= localTimestamp;
        this.userId=(String) payload.get("userId");
        this.source = (String) payload.get("source");
    }
}
