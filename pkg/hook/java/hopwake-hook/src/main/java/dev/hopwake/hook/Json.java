package dev.hopwake.hook;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Just enough JSON for the hook: writing strings, and reading the alias list. */
final class Json {
    private Json() {
    }

    static void string(StringBuilder b, String s) {
        b.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': b.append("\\\""); break;
                case '\\': b.append("\\\\"); break;
                case '\n': b.append("\\n"); break;
                case '\r': b.append("\\r"); break;
                case '\t': b.append("\\t"); break;
                default:
                    if (c < 0x20) {
                        b.append(String.format("\\u%04x", (int) c));
                    } else {
                        b.append(c);
                    }
            }
        }
        b.append('"');
    }

    /** Reads an array of objects whose values are strings; other values are skipped. */
    static List<Map<String, String>> objects(String s) {
        Reader r = new Reader(s);
        List<Map<String, String>> out = new ArrayList<>();
        r.skip();
        r.expect('[');
        r.skip();
        if (r.peek() == ']') {
            return out;
        }
        while (true) {
            r.skip();
            r.expect('{');
            Map<String, String> m = new HashMap<>();
            r.skip();
            if (r.peek() != '}') {
                while (true) {
                    r.skip();
                    String k = r.string();
                    r.skip();
                    r.expect(':');
                    r.skip();
                    if (r.peek() == '"') {
                        m.put(k, r.string());
                    } else {
                        r.scalar();
                    }
                    r.skip();
                    if (r.peek() == ',') {
                        r.pos++;
                        continue;
                    }
                    break;
                }
            }
            r.skip();
            r.expect('}');
            out.add(m);
            r.skip();
            if (r.peek() == ',') {
                r.pos++;
                continue;
            }
            r.expect(']');
            return out;
        }
    }

    private static final class Reader {
        final String s;
        int pos;

        Reader(String s) {
            this.s = s;
        }

        char peek() {
            if (pos >= s.length()) {
                throw new IllegalArgumentException("unexpected end of JSON");
            }
            return s.charAt(pos);
        }

        void skip() {
            while (pos < s.length() && Character.isWhitespace(s.charAt(pos))) {
                pos++;
            }
        }

        void expect(char c) {
            if (peek() != c) {
                throw new IllegalArgumentException("expected '" + c + "' at " + pos);
            }
            pos++;
        }

        void scalar() {
            while (pos < s.length() && ",}] \n\r\t".indexOf(s.charAt(pos)) < 0) {
                pos++;
            }
        }

        String string() {
            expect('"');
            StringBuilder b = new StringBuilder();
            while (true) {
                char c = peek();
                pos++;
                if (c == '"') {
                    return b.toString();
                }
                if (c != '\\') {
                    b.append(c);
                    continue;
                }
                char e = peek();
                pos++;
                switch (e) {
                    case 'n': b.append('\n'); break;
                    case 'r': b.append('\r'); break;
                    case 't': b.append('\t'); break;
                    case 'b': b.append('\b'); break;
                    case 'f': b.append('\f'); break;
                    case 'u':
                        b.append((char) Integer.parseInt(s.substring(pos, pos + 4), 16));
                        pos += 4;
                        break;
                    default: b.append(e);
                }
            }
        }
    }
}
