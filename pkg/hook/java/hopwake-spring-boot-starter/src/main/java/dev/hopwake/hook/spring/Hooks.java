package dev.hopwake.hook.spring;

import dev.hopwake.hook.Hook;
import dev.hopwake.hook.HookConfig;
import dev.hopwake.hook.Op;
import dev.hopwake.hook.Reading;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.factory.DisposableBean;

/**
 * The application's hooks, one per hop, made on first use and closed with the context.
 *
 * <pre>
 * private final Hook stored;
 * MyService(Hooks hooks) { this.stored = hooks.hook("hook:lvc.stored"); }
 * </pre>
 */
public class Hooks implements DisposableBean {
    private final HopwakeProperties props;
    private final String defaultService;
    private final Map<String, Hook> hooks = new ConcurrentHashMap<>();

    public Hooks(HopwakeProperties props, String defaultService) {
        this.props = props;
        this.defaultService = defaultService;
    }

    /** Whether hooks send anything. */
    public boolean enabled() {
        return props.isEnabled();
    }

    /** The hook for one hop. With hopwake.enabled=false it is null. */
    public Hook hook(String hop) {
        if (!props.isEnabled()) {
            return null;
        }
        return hooks.computeIfAbsent(hop, h -> {
            HookConfig c = HookConfig.forHop(h)
                    .tenant(props.getTenant())
                    .endpoint(props.getEndpoint())
                    .timePrecision(props.getTimePrecision())
                    .aliases(props.getApi());
            String svc = props.getService() != null ? props.getService() : defaultService;
            if (svc != null && !svc.isEmpty()) {
                c.service(svc);
            }
            return Hook.create(c);
        });
    }

    /**
     * Marks one reading at a hop. Never throws; returns false when nothing was sent (hooks off, or
     * an alias Hopwake has not learned yet).
     */
    public boolean mark(String hop, Op op, Reading reading) {
        try {
            Hook h = hook(hop);
            return h != null && h.mark(op, reading);
        } catch (RuntimeException e) {
            return false;
        }
    }

    @Override
    public void destroy() {
        hooks.values().forEach(Hook::close);
        hooks.clear();
    }
}
