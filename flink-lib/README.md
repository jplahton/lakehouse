# Flink Extra JARs

Drop JAR files here before starting the analytics stack. They are mounted into `/opt/flink/lib/extra/` inside both the jobmanager and taskmanager containers.

Two JARs are required: one for S3/MinIO connectivity, one for Iceberg table support.

---

## JAR 1 — flink-s3-fs-hadoop

Already bundled inside the Flink Docker image. No download needed — copy it out of the container.

```bash
# Start the stack first
docker compose -f docker-compose.analytics.yml up -d

# Check the exact filename
docker exec flink-jobmanager ls /opt/flink/opt/ | grep s3

# Copy it here
docker cp flink-jobmanager:/opt/flink/opt/flink-s3-fs-hadoop-2.1.jar ./flink-lib/
```

Replace `2.1` with the exact filename shown in the `ls` output.

---

## JAR 2 — iceberg-flink-runtime

From Maven Central. Must match your Flink version.

```bash
# Download — Flink 2.1, adjust ICEBERG_VERSION if a newer release is available
FLINK_VERSION=2.1
ICEBERG_VERSION=1.7.1

curl -O "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-${FLINK_VERSION}/${ICEBERG_VERSION}/iceberg-flink-runtime-${FLINK_VERSION}-${ICEBERG_VERSION}.jar"

mv iceberg-flink-runtime-*.jar ./flink-lib/
```

Browse available versions at:
```
https://repo1.maven.org/maven2/org/apache/iceberg/
```
Look for entries named `iceberg-flink-runtime-<your-flink-version>/`.

---

## Apply changes

Restart Flink after adding JARs:

```bash
docker compose -f docker-compose.analytics.yml restart
```

Verify they are loaded:

```bash
docker exec flink-jobmanager ls /opt/flink/lib/extra/
```

---

## Troubleshooting

**JAR not found after restart**
Check the volume mount in `docker-compose.analytics.yml` — both jobmanager and taskmanager must mount `./flink-lib:/opt/flink/lib/extra`.

**ClassNotFoundException at job submission**
The JAR is in `extra/` but Flink only auto-loads from `lib/`. Either move the JAR directly to `lib/` in the compose volume mount, or submit jobs with `--classpath` pointing to the extra path.

**Version mismatch error**
`iceberg-flink-runtime` must match the Flink major.minor version exactly. A JAR built for Flink 1.20 will not work with Flink 2.1. The compose file is pinned to `apache/flink:2.1` — do not change it without also updating the Iceberg JAR.
