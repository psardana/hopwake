# Hopwake hook for Java

Two artifacts, both Java 17 or later:

- `hopwake-hook`: the hook itself. No dependencies.
- `hopwake-spring-boot-starter`: the hook as Spring beans, for Spring Boot 3 and 4. It can also
  mark what `@KafkaListener` methods handled and what `KafkaTemplate` sent.

The contract all hooks follow, in every language, is in [docs/hooks.md](../../../docs/hooks.md).

## Plain Java

```java
Hook stored = Hook.create(HookConfig.forHop("hook:lvc.stored").tenant("fleet"));

// a reading with a sequence number
stored.mark(Op.STORE, "dev-00042", 1683);

// a reading known by its message type and the time the device gave it,
// teaching Hopwake that this device is system 100042 further down
stored.markTimeTeach(Op.STORE, serial, "systemId", "100042", "ems.measurements.Measurement", measuredAt);

// a later hop that only knows the system ID; false while Hopwake has not learned it
served.markTimeAlias(Op.SERVE, "systemId", "100042", "ems.measurements.Measurement", measuredAt);
```

Marks are queued and sent once a second from a daemon thread. A mark never blocks or throws;
when Hopwake cannot be reached, marks are dropped and counted (`dropped()`). Close the hook on
shutdown to send what is left.

## Spring Boot

Add the starter and set the tenant:

```yaml
hopwake:
  tenant: fleet
  time-precision: 1s          # as time_precision in the rules file
  # endpoint and api default to HOPWAKE_OTLP_ENDPOINT and HOPWAKE_API
```

Then take hooks from the `Hooks` bean:

```java
@Service
class LastValues {
    private final Hooks hooks;
    LastValues(Hooks hooks) { this.hooks = hooks; }

    void store(String serial, long systemId, Measurement m) {
        cache.put(serial, m);
        hooks.mark("hook:lvc.stored", Op.STORE,
                Reading.time(serial, "ems.measurements.Measurement", toInstant(m.getMeasurementTime()))
                       .teach("systemId", Long.toString(systemId)));
    }
}
```

`hopwake.enabled: false` turns every hook into a no-op. The service name on the spans is
`spring.application.name` unless `hopwake.service` is set.

### Marking Kafka topics

The starter can mark records itself: a record a listener handled without an exception
(`on: consume`, op `receive`) or a record the broker acknowledged (`on: send`, op `route`).
It works with record and batch listeners, and keeps any interceptor or producer listener the
application set.

```yaml
hopwake:
  kafka:
    marks:
      - topic: ems.measurements.raw
        hop: hook:measurement-service.received
      - topic: tsdb.ingress.raw
        hop: hook:measurement-service.sent
        on: send
```

Each hop must be a hook source in the rules file. The starter needs to know which reading a record
carries. By default it reads headers (`hopwake.kafka.device-header` or the record key, and
`seq-header`, or `time-header` with `type-header`). When the reading is identified by the payload,
define a `KafkaReadingResolver` bean:

```java
@Bean
KafkaReadingResolver readings() {
    return (topic, key, value, headers) -> value instanceof Ems.Measurement m
            ? Reading.time((String) key, "ems.measurements.Measurement", toInstant(m.getMeasurementTime()))
            : null;
}
```

Records it returns null for are not marked.

## Building and testing

```bash
mvn verify            # Spring Boot 3
mvn -Pboot4 verify    # Spring Boot 4
```

The tests check every journey ID in the shared vectors and run the starter against an embedded
Kafka broker. The wire check runs the conformance program against `cmd/hookcheck`:

```bash
scripts/check-hooks.sh java
```
