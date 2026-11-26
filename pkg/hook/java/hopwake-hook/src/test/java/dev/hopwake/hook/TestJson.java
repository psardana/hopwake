package dev.hopwake.hook;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** A small JSON reader for the shared test vectors: objects, arrays, strings, numbers, literals. */
final class TestJson {
    private final String s;
    private int pos;

    private TestJson(String s) {
        this.s = s;
    }

    static Object parse(String s) {
        TestJson p = new TestJson(s);
        Object v = p.value();
        p.ws();
        if (p.pos != s.length()) {
            throw new IllegalArgumentException("trailing data at " + p.pos);
        }
        return v;
    }

    private void ws() {
        while (pos < s.length() && Character.isWhitespace(s.charAt(pos))) {
            pos++;
        }
    }

    private Object value() {
        ws();
        char c = s.charAt(pos);
        if (c == '{') {
            pos++;
            Map<String, Object> m = new LinkedHashMap<>();
            ws();
            if (s.charAt(pos) == '}') {
                pos++;
                return m;
            }
            while (true) {
                ws();
                String k = (String) value();
                ws();
                pos++; // ':'
                m.put(k, value());
                ws();
                if (s.charAt(pos++) == '}') {
                    return m;
                }
            }
        }
        if (c == '[') {
            pos++;
            List<Object> l = new ArrayList<>();
            ws();
            if (s.charAt(pos) == ']') {
                pos++;
                return l;
            }
            while (true) {
                l.add(value());
                ws();
                if (s.charAt(pos++) == ']') {
                    return l;
                }
            }
        }
        if (c == '"') {
            pos++;
            StringBuilder b = new StringBuilder();
            while (true) {
                char x = s.charAt(pos++);
                if (x == '"') {
                    return b.toString();
                }
                if (x == '\\') {
                    char e = s.charAt(pos++);
                    if (e == 'u') {
                        b.append((char) Integer.parseInt(s.substring(pos, pos + 4), 16));
                        pos += 4;
                    } else {
                        b.append(e == 'n' ? '\n' : e == 't' ? '\t' : e == 'r' ? '\r' : e);
                    }
                } else {
                    b.append(x);
                }
            }
        }
        int start = pos;
        while (pos < s.length() && ",}] \n\r\t".indexOf(s.charAt(pos)) < 0) {
            pos++;
        }
        String lit = s.substring(start, pos);
        switch (lit) {
            case "true": return true;
            case "false": return false;
            case "null": return null;
            default: return Double.parseDouble(lit);
        }
    }
}
