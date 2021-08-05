package streaming.utils;

import java.util.Map;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class JsonDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    private Class<T> tClass;

    public JsonDeserializer(Class<T> tClass) {
        this.tClass = tClass;
    }

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        // nothing to do
    }
    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        T data;
        try {

            data = objectMapper.readValue(bytes, tClass);
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return data;
    }
    @Override
    public void close() {
        // nothing to do
    }

}
