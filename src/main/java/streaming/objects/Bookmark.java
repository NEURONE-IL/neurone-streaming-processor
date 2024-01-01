package streaming.objects;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import streaming.interfaces.MetadataProvider;
public class Bookmark  implements MetadataProvider{
    public String url;
    public String action;
    public Boolean relevant;
    public Boolean userMade;
    public String userId;
    public String studyId;

    @JsonProperty("payload")
    private void unpackNested(Map<String,Object> payload) {
        this.url=(String)payload.get("url");
        this.action=(String) payload.get("action");
        this.relevant=(Boolean) payload.get("relevant");
        this.userMade=(Boolean) payload.get("userMade");
        this.userId=(String) payload.get("userId");
        this.studyId=(String) payload.get("studyId");
    }

    @Override
    public Metadata getMetadata() {
        return new Metadata(this.userId);
    }

    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return "["+this.userId+","+this.action+","+this.url+"]";
    }
}
