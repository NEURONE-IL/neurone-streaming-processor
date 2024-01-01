package streaming.objects;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import streaming.interfaces.MetadataProvider;

public class Event  implements MetadataProvider{
    public String url;
    public String type;
    public Double localTimestamp;
    public String userId;
    public String source;
    public String studyId;

    @JsonProperty("payload")
    private void unpackNested(Map<String, Object> payload) {
        this.url = (String) payload.get("url");
        this.type = (String) payload.get("type");
        Long localTimestamp = (Long) payload.get("localTimeStamp");
        this.localTimestamp = localTimestamp.doubleValue();
        this.userId = (String) payload.get("userId");
        this.source = (String) payload.get("source");
        this.studyId = (String) payload.get("studyId");
    }

    @Override
    public Metadata getMetadata() {
        return new Metadata(this.studyId);   
    }
}
