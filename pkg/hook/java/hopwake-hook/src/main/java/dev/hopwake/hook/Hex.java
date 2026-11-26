package dev.hopwake.hook;

final class Hex {
    private static final char[] DIGITS = "0123456789abcdef".toCharArray();

    private Hex() {
    }

    static String of(byte[] b) {
        char[] out = new char[b.length * 2];
        for (int i = 0; i < b.length; i++) {
            out[2 * i] = DIGITS[(b[i] >> 4) & 0xf];
            out[2 * i + 1] = DIGITS[b[i] & 0xf];
        }
        return new String(out);
    }
}
