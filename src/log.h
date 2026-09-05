#ifndef LOG_H
#define LOG_H

/*
 * Minimal logger. Every line is prefixed with:
 *   <ISO8601 timestamp with tz offset> <file>:<func>:<line> <message>
 */

void log_write(const char *file, const char *func, int line, const char *fmt, ...)
#if defined(__GNUC__) || defined(__clang__)
    __attribute__((format(printf, 4, 5)))
#endif
    ;

#define LOG(...) log_write(__FILE__, __func__, __LINE__, __VA_ARGS__)

#endif
