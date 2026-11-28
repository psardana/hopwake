# Running the tap against a real broker and cluster

The tap only reads. It subscribes to MQTT like any other client, at QoS 0,
and reads Kafka without ever producing. This page lists what it needs to do
that on a broker and cluster that are locked down.

## Settings

| Setting | Flag | Environment |
| --- | --- | --- |
| MQTT broker URL; `ssl://` or `mqtts://` for TLS | `-mqtt` | `TAP_MQTT` |
| MQTT username | `-mqtt-user` | `TAP_MQTT_USER` |
| MQTT password | | `TAP_MQTT_PASSWORD` |
| CA that signed the broker's certificate (PEM), when not a public one | `-mqtt-ca` | `TAP_MQTT_CA` |
| MQTT client certificate and key (PEM), for brokers that check client certificates | `-mqtt-cert`, `-mqtt-key` | `TAP_MQTT_CERT`, `TAP_MQTT_KEY` |
| Join an MQTT 5 shared subscription group, so several taps split the messages | `-mqtt-share` | `TAP_MQTT_SHARE` |
| Kafka bootstrap servers | `-kafka` | `TAP_KAFKA` |
| Kafka SASL mechanism: `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512` | `-kafka-sasl` | `TAP_KAFKA_SASL` |
| Kafka username | `-kafka-user` | `TAP_KAFKA_USER` |
| Kafka password | | `TAP_KAFKA_PASSWORD` |
| Kafka over TLS | `-kafka-tls` | `TAP_KAFKA_TLS=true` |
| CA that signed the brokers' certificates (PEM) | `-kafka-ca` | `TAP_KAFKA_CA` |
| Kafka client certificate and key (PEM), for clusters that authenticate by TLS, such as Strimzi users of type `tls` | `-kafka-cert`, `-kafka-key` | `TAP_KAFKA_CERT`, `TAP_KAFKA_KEY` |
| Kafka consumer group; `none` reads without one | `-group` | `TAP_GROUP` |
| Send hop events for one reading in N (counters always cover all) | `-sample` | `TAP_SAMPLE` |

Passwords are read only from the environment, so they never show in a
process list.

## The rights it needs

**MQTT:** read (subscribe) on the topic filters in the rules file, and the
device mark topics if there are any. No publish rights. A broker that
refuses a filter is logged at start:
`mqtt: the broker refused plant/# (reason 0x87); check the tap's read rights`.

**Kafka, without a consumer group** (`TAP_GROUP=none`): `Read` and
`Describe` on each topic in the rules file, nothing else. The tap reads
every partition itself, from the end, and commits nothing, so it leaves no
trace in the cluster. This is the smallest grant and the right start for a
QA or production cluster.

**Kafka, with a consumer group:** also `Read` on that group. Several taps
in one group split the partitions between them.

Neither needs `Write`, `Create`, `Alter` or `IdempotentWrite`. The rig in
`reference/qa` grants exactly these rights and checks that writing is
refused (`scripts/qa-tap.sh --rights`).

## More than one tap

One tap reads MQTT in a single loop, and Kafka in another. When one is not
enough:

- MQTT: start several taps with the same `TAP_MQTT_SHARE`. The broker hands
  each message to one of them.
- Kafka: give them one `TAP_GROUP`, so each reads some of the partitions.
- Taps that split traffic still agree on journey IDs and on sampling, since
  both come from the reading itself.

A tap that cannot keep up drops messages from its own queue rather than slow
the broker down, and says so in its health reports. The journey view then
treats those hops as not fully watched, instead of blaming the pipeline.
`scripts/qa-tap.sh` measures where that starts on a given machine.
