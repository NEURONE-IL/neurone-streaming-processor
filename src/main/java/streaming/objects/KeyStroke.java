package streaming.objects;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class KeyStroke {
    
    public String username;
    public String url;
    public Double localTimestamp;
    public int keyCode;

    @JsonProperty("payload")
    private void unpackNested(Map<String,Object> payload){
        this.username = (String)payload.get("username");
        this.url=(String)payload.get("url");
        Long localTimestamp= (Long) payload.get("localTimeStamp");
        this.localTimestamp= localTimestamp.doubleValue();
        this.keyCode= (int) payload.get("keyCode");
    }


}
