package lakehouse;

import lakehouse.model.SensorReading;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.table.data.*;

import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;

import org.apache.hadoop.conf.Configuration;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Generates synthetic sensor data and writes it to an Iceberg table in MinIO via Nessie.
 * No external broker or process needed — validates the full pipeline with simulated data.
 *
 * Config (same .env as the rest of the stack):
 *   STORAGE_NODE_IP     LAN IP of storage node  (default: localhost)
 *   MINIO_ROOT_USER     MinIO access key         (default: minioadmin)
 *   MINIO_ROOT_PASSWORD MinIO secret             (default: minioadmin)
 *   SIM_DEVICES         virtual devices          (default: 10)
 *   SIM_RATE_PER_SEC    records/sec per device   (default: 10)
 */
public class SensorGeneratorJob {

    public static void main(String[] args) throws Exception {

        // ── Config ───────────────────────────────────────────────────────────
        String storageIp     = env("STORAGE_NODE_IP",     "localhost");
        String minioUser     = env("MINIO_ROOT_USER",     "minioadmin");
        String minioPassword = env("MINIO_ROOT_PASSWORD", "minioadmin");
        int    numDevices    = Integer.parseInt(env("SIM_DEVICES",        "50"));
        double ratePerSec    = Double.parseDouble(env("SIM_RATE_PER_SEC", "100"));

        String minioEndpoint = "http://" + storageIp + ":9000";
        String nessieUri     = "http://" + storageIp + ":19120/api/v2";

        System.out.printf("Generating %d device(s) at %.1f record/s each → Iceberg%n",
            numDevices, ratePerSec);

        // ── Flink environment ────────────────────────────────────────────────
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000);
        env.setParallelism(1);

        // ── Hadoop / S3A config ──────────────────────────────────────────────
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.s3a.endpoint",         minioEndpoint);
        hadoopConf.set("fs.s3a.access.key",        minioUser);
        hadoopConf.set("fs.s3a.secret.key",        minioPassword);
        hadoopConf.set("fs.s3a.path.style.access", "true");
        hadoopConf.set("fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem");

        // ── Nessie catalog ───────────────────────────────────────────────────
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("type",                 "nessie");
        catalogProps.put("uri",                  nessieUri);
        catalogProps.put("ref",                  "main");
        catalogProps.put("warehouse",            "s3a://lakehouse/");
        // HadoopFileIO uses the S3A connector (flink-s3-fs-hadoop JAR already in flink-lib/)
        // S3FileIO would require AWS SDK v2 jars which are not in the Flink classpath
        catalogProps.put("io-impl", "org.apache.iceberg.hadoop.HadoopFileIO");

        CatalogLoader catalogLoader = CatalogLoader.custom(
            "nessie", catalogProps, hadoopConf,
            "org.apache.iceberg.nessie.NessieCatalog"
        );

        // ── Create table if missing ──────────────────────────────────────────
        TableIdentifier tableId = TableIdentifier.of("sensors");
        Schema schema = new Schema(
            Types.NestedField.required(1, "device_id", Types.StringType.get()),
            Types.NestedField.required(2, "ts",        Types.TimestampType.withZone()),
            Types.NestedField.optional(3, "temp_c",    Types.DoubleType.get()),
            Types.NestedField.optional(4, "humidity",  Types.DoubleType.get()),
            Types.NestedField.optional(5, "battery",   Types.DoubleType.get()),
            Types.NestedField.optional(6, "topic",     Types.StringType.get())
        );
        PartitionSpec spec = PartitionSpec.builderFor(schema).day("ts").build();

        Catalog catalog = catalogLoader.loadCatalog();
        if (!catalog.tableExists(tableId)) {
            catalog.createTable(tableId, schema, spec);
        }

        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);

        // ── Source ───────────────────────────────────────────────────────────
        long intervalMs = Math.max(1, Math.round(1000.0 / ratePerSec));

        DataStream<SensorReading> readings = env
            .addSource(new SensorSource(numDevices, intervalMs))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                    .withTimestampAssigner((r, t) -> r.ts.toEpochMilli())
            );

        // ── Map to Iceberg RowData ────────────────────────────────────────────
        DataStream<RowData> rows = readings.map(r -> {
            GenericRowData row = new GenericRowData(6);
            row.setField(0, StringData.fromString(r.deviceId));
            row.setField(1, TimestampData.fromInstant(r.ts));
            row.setField(2, r.tempC);
            row.setField(3, r.humidity);
            row.setField(4, r.battery);
            row.setField(5, StringData.fromString(r.topic));
            return (RowData) row;
        });

        // ── Iceberg sink ─────────────────────────────────────────────────────
        FlinkSink.forRowData(rows)
            .tableLoader(tableLoader)
            .append();

        env.execute("Sensor Generator → Iceberg");
    }

    // ── Sensor source ─────────────────────────────────────────────────────────

    public static class SensorSource extends RichSourceFunction<SensorReading> {

        private final int numDevices;
        private final long intervalMs;
        private volatile boolean running = true;

        public SensorSource(int numDevices, long intervalMs) {
            this.numDevices  = numDevices;
            this.intervalMs  = intervalMs;
        }

        @Override
        public void run(SourceFunction.SourceContext<SensorReading> ctx) throws Exception {
            Random rng  = new Random();
            long   tick = 0;

            while (running) {
                long now = System.currentTimeMillis();

                for (int slot = 0; slot < numDevices; slot++) {
                    SensorReading r = new SensorReading();
                    r.deviceId = String.format("sensor-%02d", slot + 1);
                    r.ts       = Instant.now();
                    r.topic    = "simulated/" + r.deviceId;

                    // Temperature: sine wave (10-min period) + noise, unique phase per device
                    r.tempC = round(20.0
                        + 4.0 * Math.sin(2 * Math.PI * tick / 600.0 + slot)
                        + rng.nextGaussian() * 0.3);

                    // Humidity: slower sine wave + noise
                    r.humidity = round(55.0
                        + 5.0 * Math.sin(2 * Math.PI * tick / 1200.0 + slot)
                        + rng.nextGaussian() * 0.5);

                    // Battery: slow drain
                    r.battery = round(Math.max(0, 100.0 - tick * 0.005 + rng.nextGaussian() * 0.05));

                    ctx.collect(r);
                }

                tick++;

                // Sleep for the remainder of the interval
                long elapsed = System.currentTimeMillis() - now;
                long sleep   = intervalMs - elapsed;
                if (sleep > 0) Thread.sleep(sleep);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        private static double round(double v) {
            return Math.round(v * 100.0) / 100.0;
        }
    }

    private static String env(String key, String def) {
        return System.getenv().getOrDefault(key, def);
    }
}
