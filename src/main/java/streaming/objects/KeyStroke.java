package streaming.objects;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class KeyStroke {
    
    public String url;
    public Double localTimestamp;
    public int keyCode;
    public String userId;

    @JsonProperty("payload")
    private void unpackNested(Map<String,Object> payload){
        this.url=(String)payload.get("url");
        Double localTimestamp= (Double) payload.get("localTimeStamp");
        this.localTimestamp= localTimestamp;
        this.keyCode= (int) payload.get("keyCode");
        this.userId=(String) payload.get("userId");
    }


}
