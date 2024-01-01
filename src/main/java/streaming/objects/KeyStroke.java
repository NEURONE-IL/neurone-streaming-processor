package streaming.objects;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import streaming.interfaces.MetadataProvider;

public class KeyStroke  implements MetadataProvider{

    public String url;
    public Double localTimestamp;
    public int keyCode;
    public String userId;
    public String studyId;

    @JsonProperty("payload")
    private void unpackNested(Map<String, Object> payload) {
        this.url = (String) payload.get("url");
        Long localTimestamp = (Long) payload.get("localTimeStamp");
        this.localTimestamp = localTimestamp.doubleValue();
        this.keyCode = (int) payload.get("keyCode");
        this.userId = (String) payload.get("userId");
        this.studyId = (String) payload.get("studyId");
    }

    @Override
    public Metadata getMetadata() {
        return new Metadata(this.userId);
    }

}
