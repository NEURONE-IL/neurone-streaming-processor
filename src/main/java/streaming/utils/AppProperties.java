package streaming.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

//By vsepulve (github)

/**
 * Class to load and store the application properties and configuration
 */
public class AppProperties {
    private static Properties props = new Properties();

    static {
        try {
            InputStream stream = AppProperties.class.getResourceAsStream("/application.properties");
            if (stream == null) {
                throw new ExceptionInInitializerError("Failed to open properties stream.");
            }
            AppProperties.props.load(stream);
            stream.close();
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }

    }

    public final static String KAFKA_BOOTSTRAP_SERVERS = props.getProperty("kafka.bootstrap.servers");

    public final static int KAFKA_CACHE_MAX_BYTES_BUFFERING_CONFIG = Integer
            .parseInt(props.getProperty("kafka.cache.max.bytes.buffering.config"));

    public final static String APPLICATION_REST_DEFAULT_HOST = props.getProperty("application.rest.default.host");

    public final static String APPLICATION_ID_CONFIG = props.getProperty("application.id.config");

    public final static String STREAMING_DB_VISITEDLINKS_TOPIC = props.getProperty("streaming.db.visitedlinks.topic");

    public final static String STREAMING_DB_BOOKMARKS_TOPIC = props.getProperty("streaming.db.bookmarks.topic");

    public final static String STREAMING_DB_KEYSTROKES_TOPIC = props.getProperty("streaming.db.keystrokes.topic");

    public final static String STREAMING_DB_QUERIES_TOPIC = props.getProperty("streaming.db.queries.topic");

    public final static String STREAMING_DB_EVENTS_TOPIC = props.getProperty("streaming.db.events.topic");

    public final static String STREAMING_STATE_TTL_STORE = props.getProperty("streaming.state.ttl.store");

    public final static String STREAMING_STATE_DEDUP_STORE = props.getProperty("streaming.state.depup.store");

    public final static String STREAMING_STATE_WRITING_TIME_STORE = props
            .getProperty("streaming.state.writing.time.store");

    public final static String STREAMING_STATE_REFERENCE_TIME_STORE = props
            .getProperty("streaming.state.reference.time.store");

    public final static String STREAMING_STATE_LAST_KEYSTROKES_STORE = props
            .getProperty("streaming.state.last.keystroke.store");

    public final static String STREAMING_STATE_TOTALCOVER_STORE = props.getProperty("streaming.state.totalcover.store");

    public final static String STREAMING_STATE_BMRELEVANT_STORE = props.getProperty("streaming.state.bmrelevant.store");

    public final static String STREAMING_STATE_PRECISION_STORE = props.getProperty("streaming.state.precision.store");

    public final static String STREAMING_STATE_PAGE_STAY_STORE = props.getProperty("streaming.state.page.stay.store");

    public final static String STREAMING_STATE_TOTAL_PAGE_STAY_STORE = props
            .getProperty("streaming.state.total.page.stay.store");

    public final static String STREAMING_STATE_PAGE_SEQUENCE_STORE = props
            .getProperty("streaming.state.page.sequence.store");

    public final static String STREAMING_STATE_METADATA_STORE = props.getProperty("streaming.state.metadata.store");

    public final static String STREAMING_FIRST_PAGE_STORE = props.getProperty("streaming.state.first.page.store");

    public final static String STREAMING_TOTALCOVER_TOPIC = props.getProperty("streaming.totalcover.topic");

    public final static String STREAMING_BMRELEVANT_TOPIC = props.getProperty("streaming.bmrelevant.topic");

    public final static String STREAMING_WRITINGTIME_TOPIC = props.getProperty("streaming.writingtime.topic");

    public final static String STREAMING_PRECISION_TOPIC = props.getProperty("streaming.precision.topic");

    public final static String STREAMING_TOTAL_PAGE_STAY_TOPIC = props.getProperty("streaming.page.stay.topic");

    public final static String STREAMING_PAGE_STAY_TOPIC = props.getProperty("streaming.total.page.stay.topic");

    public final static String STREAMING_IF_QUOTES_TOPIC = props.getProperty("streaming.total.ifquotes.topic");

    public final static String STREAMING_FIRST_QUERY_TIME_TOPIC = props.getProperty("streaming.firstquerytime.topic");

    public final static String STREAMING_CHALLENGE_STARTED_TOPIC = props
            .getProperty("streaming.challengestarted.topic");
}
