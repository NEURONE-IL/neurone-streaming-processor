package streaming.objects;

public class Metadata {
    public String studyId;

    public Metadata(String studyId) {
        this.studyId = studyId;
    }

    // No-argument constructor
    public Metadata() {
    }

    // Getter and setter methods
    public String getStudyId() {
        return studyId;
    }

    public void setStudyId(String studyId) {
        this.studyId = studyId;
    }

}
