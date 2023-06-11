package streaming.objects;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Query {
    public String url;
    public Double localTimestamp;
    public String query;
    public String userId;

    @JsonProperty("payload")
    private void unpackNested(Map<String, Object> payload) {
        this.url = (String) payload.get("url");
        Long localTimestamp = (Long) payload.get("localTimeStamp");
        this.localTimestamp = localTimestamp.doubleValue();
        this.query = (String) payload.get("query");
        this.userId = (String) payload.get("userId");
    }
}
