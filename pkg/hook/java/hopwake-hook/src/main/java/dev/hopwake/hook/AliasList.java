package dev.hopwake.hook;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** The pairs Hopwake learned, such as which system ID stands for which device, refreshed every 5 s. */
final class AliasList {
    private final URI uri;
    private final HttpClient http;
    private volatile Map<String, String> pairs = Map.of();

    AliasList(String base, HttpClient http, ScheduledExecutorService timer) {
        String b = base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
        this.uri = URI.create(b + "/api/aliases");
        this.http = http;
        timer.scheduleWithFixedDelay(this::refresh, 0, 5, TimeUnit.SECONDS);
    }

    String device(String kind, String value) {
        return pairs.get(Aliases.format(kind, value));
    }

    private void refresh() {
        try {
            HttpRequest req = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(3)).GET().build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                return;
            }
            Map<String, String> m = new HashMap<>();
            for (Map<String, String> p : Json.objects(resp.body())) {
                String k = p.get("kind"), v = p.get("value"), d = p.get("device");
                if (k != null && v != null && d != null) {
                    m.put(Aliases.format(k, v), d);
                }
            }
            pairs = m;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            // Keep the list we have.
        }
    }
}
