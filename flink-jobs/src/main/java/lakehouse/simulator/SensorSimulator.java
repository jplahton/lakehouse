package lakehouse.simulator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.eclipse.paho.client.mqttv3.*;

import java.time.Instant;
import java.util.Random;
import java.util.concurrent.*;

/**
 * Standalone simulator — publishes synthetic sensor data to an MQTT broker.
 * Run this to test the Flink pipeline without real IoT devices.
 *
 * Config via environment variables (same .env as the rest of the stack):
 *   MQTT_HOST         broker LAN IP       (default: localhost)
 *   MQTT_PORT         broker port         (default: 1883)
 *   MQTT_TOPIC_PREFIX topic prefix        (default: sensors)
 *   SIM_DEVICES       number of devices   (default: 5)
 *   SIM_INTERVAL_MS   publish interval ms (default: 1000)
 *
 * Usage:
 *   java -cp target/flink-jobs-*.jar lakehouse.simulator.SensorSimulator
 */
public class SensorSimulator {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String host        = env("MQTT_HOST",         "localhost");
        int    port        = Integer.parseInt(env("MQTT_PORT",          "1883"));
        String topicPrefix = env("MQTT_TOPIC_PREFIX", "sensors");
        int    numDevices  = Integer.parseInt(env("SIM_DEVICES",        "5"));
        long   intervalMs  = Long.parseLong(env("SIM_INTERVAL_MS",     "1000"));

        String brokerUrl = "tcp://" + host + ":" + port;
        System.out.printf("Connecting to %s — %d device(s), interval %d ms%n",
            brokerUrl, numDevices, intervalMs);

        MqttClient client = new MqttClient(brokerUrl, "sensor-simulator");
        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setAutomaticReconnect(true);
        opts.setCleanSession(true);
        client.connect(opts);
        System.out.println("Connected.");

        Device[] devices = new Device[numDevices];
        for (int i = 0; i < numDevices; i++) {
            devices[i] = new Device(String.format("sensor-%02d", i + 1));
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down simulator.");
            try { client.disconnect(); } catch (MqttException ignored) {}
        }));

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            for (Device device : devices) {
                try {
                    String topic   = topicPrefix + "/" + device.id;
                    byte[] payload = device.nextPayload();
                    client.publish(topic, new MqttMessage(payload));
                    System.out.printf("[%s] %s → %s%n",
                        Instant.now(), topic, new String(payload));
                } catch (Exception e) {
                    System.err.println("Publish error: " + e.getMessage());
                }
            }
        }, 0, intervalMs, TimeUnit.MILLISECONDS);

        // Block until interrupted
        Thread.currentThread().join();
    }

    // ── Simulated device ─────────────────────────────────────────────────────

    static class Device {
        final String id;
        final Random rng = new Random();

        double battery  = 100.0;       // drains slowly
        double tempBase = 18 + rng.nextDouble() * 8;   // 18–26 °C baseline per device
        double humBase  = 50 + rng.nextDouble() * 20;  // 50–70 % baseline per device
        long   tick     = 0;

        Device(String id) { this.id = id; }

        byte[] nextPayload() throws Exception {
            tick++;

            // Temperature: sine wave (10-min period) + Gaussian noise
            double tempC = tempBase
                + 3.0 * Math.sin(2 * Math.PI * tick / 600)
                + rng.nextGaussian() * 0.2;

            // Humidity: slow random walk + noise
            humBase += rng.nextGaussian() * 0.05;
            humBase  = Math.max(20, Math.min(90, humBase));
            double humidity = humBase + rng.nextGaussian() * 0.5;

            // Battery: slow drain with tiny noise
            battery -= 0.005 + rng.nextDouble() * 0.002;
            battery  = Math.max(0, battery);

            ObjectNode node = MAPPER.createObjectNode();
            node.put("device_id", id);
            node.put("temp_c",    Math.round(tempC    * 100.0) / 100.0);
            node.put("humidity",  Math.round(humidity * 100.0) / 100.0);
            node.put("battery",   Math.round(battery  * 100.0) / 100.0);
            return MAPPER.writeValueAsBytes(node);
        }
    }

    private static String env(String key, String def) {
        return System.getenv().getOrDefault(key, def);
    }
}
