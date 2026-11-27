/* Prints device mark messages for the marks given on the command line, one
 * message per line, so the tap side can check them (internal/devmark):
 *
 *   emit <cap> time <hop> <op> <type> <time_ms> <at_ms> ...
 *   emit <cap> seq  <hop> <op> <seq> <boot> <at_ms> ...
 */
#include "../hopwake_mark.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char **argv) {
    static hw_mark slots[64];
    hw_queue q;
    char buf[4096];
    size_t n;
    if (argc < 2) {
        return 2;
    }
    hw_queue_init(&q, slots, (size_t)atoi(argv[1]));
    for (int i = 2; i + 5 < argc + 1; ) {
        hw_op op = strcmp(argv[i + 2], "store") == 0 ? HW_STORE : HW_SEND;
        if (strcmp(argv[i], "time") == 0 && i + 5 < argc) {
            hw_mark_time(&q, argv[i + 1], op, argv[i + 3], strtoll(argv[i + 4], NULL, 10), strtoll(argv[i + 5], NULL, 10));
        } else if (strcmp(argv[i], "seq") == 0 && i + 5 < argc) {
            hw_mark_seq(&q, argv[i + 1], op, strtoull(argv[i + 3], NULL, 10), argv[i + 4], strtoll(argv[i + 5], NULL, 10));
        } else {
            return 2;
        }
        i += 6;
    }
    while (hw_encode(&q, buf, sizeof buf, &n) > 0) {
        printf("%s\n", buf);
        hw_commit(&q, n);
    }
    return 0;
}
