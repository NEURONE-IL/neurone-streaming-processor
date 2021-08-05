package streaming.objects;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class VisitedLink {
    public String username;
    public String url;

    @JsonProperty("payload")
    private void unpackNested(Map<String,Object> payload) {
        this.username = (String)payload.get("username");
        this.url=(String)payload.get("url");
    }
    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return "D11";
    }
}
