#define _POSIX_C_SOURCE 200809L // expose clock_gettime/CLOCK_REALTIME on glibc

#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>

#include "log.h"

static const char *basename_of(const char *path)
{
    const char *slash = strrchr(path, '/');
    return slash ? slash + 1 : path;
}

void log_write(const char *file, const char *func, int line, const char *fmt, ...)
{
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);

    struct tm tmv;
    localtime_r(&now.tv_sec, &tmv);

    char ts[40];
    strftime(ts, sizeof(ts), "%Y-%m-%dT%H:%M:%S%z", &tmv);

    fprintf(stderr, "%s %s:%s:%d ", ts, basename_of(file), func, line);

    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
}
