package streaming.objects;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import streaming.interfaces.MetadataProvider;

public class Query implements MetadataProvider {
    public String url;
    public Double localTimestamp;
    public String query;
    public String userId;
    public String studyId;

    @JsonProperty("payload")
    private void unpackNested(Map<String, Object> payload) {
        this.url = (String) payload.get("url");
        Long localTimestamp = (Long) payload.get("localTimeStamp");
        this.localTimestamp = localTimestamp.doubleValue();
        this.query = (String) payload.get("query");
        this.userId = (String) payload.get("userId");
        this.studyId = (String) payload.get("studyId");
    }

    @Override
    public Metadata getMetadata() {
        return new Metadata(this.userId);
    }
}
