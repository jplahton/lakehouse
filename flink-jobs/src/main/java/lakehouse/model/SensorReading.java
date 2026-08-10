package lakehouse.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.time.Instant;

public class SensorReading implements Serializable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public String deviceId;
    public Instant ts;
    public Double tempC;
    public Double humidity;
    public Double battery;
    public String topic;

    public static SensorReading parse(String topic, byte[] payload) throws Exception {
        JsonNode node = MAPPER.readTree(payload);
        SensorReading r = new SensorReading();
        r.topic    = topic;
        r.ts       = Instant.now();
        r.deviceId = node.path("device_id").asText(topic);
        r.tempC    = node.has("temp_c")   ? node.get("temp_c").asDouble()   : null;
        r.humidity = node.has("humidity") ? node.get("humidity").asDouble() : null;
        r.battery  = node.has("battery")  ? node.get("battery").asDouble()  : null;
        return r;
    }
}
