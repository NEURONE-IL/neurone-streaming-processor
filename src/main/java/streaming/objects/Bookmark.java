package streaming.objects;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
public class Bookmark {
    public String url;
    public String action;
    public Boolean relevant;
    public Boolean userMade;
    public String userId;


    @JsonProperty("payload")
    private void unpackNested(Map<String,Object> payload) {
        this.url=(String)payload.get("url");
        this.action=(String) payload.get("action");
        this.relevant=(Boolean) payload.get("relevant");
        this.userMade=(Boolean) payload.get("userMade");
        this.userId=(String) payload.get("userId");
    }

    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return "["+this.userId+","+this.action+","+this.url+"]";
    }
}
