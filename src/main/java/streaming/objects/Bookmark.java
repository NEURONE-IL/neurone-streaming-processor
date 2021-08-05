package streaming.objects;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
public class Bookmark {
    public String username;
    public String url;
    public String action;
    public Boolean relevant;
    public Boolean userMade;


    @JsonProperty("payload")
    private void unpackNested(Map<String,Object> payload) {
        this.username = (String)payload.get("username");
        this.url=(String)payload.get("url");
        this.action=(String) payload.get("action");
        this.relevant=(Boolean) payload.get("relevant");
        this.userMade=(Boolean) payload.get("userMade");
    }

    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return "["+this.username+","+this.action+","+this.url+"]";
    }
}
