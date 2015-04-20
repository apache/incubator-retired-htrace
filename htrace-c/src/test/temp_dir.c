/*
 * Copyright 2011-2012 the Redfish authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "test/temp_dir.h"
#include "util/log.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// Globals
static int g_tempdir_nonce = 0;

static int g_num_tempdirs = 0;

static char **g_tempdirs = NULL;

static pthread_mutex_t tempdir_lock = PTHREAD_MUTEX_INITIALIZER;

// Functions
static void cleanup_registered_tempdirs(void)
{
    int i;
    const char *skip_cleanup;
    skip_cleanup = getenv("SKIP_CLEANUP");
    if (skip_cleanup)
        return;
    pthread_mutex_lock(&tempdir_lock);
    for (i = 0; i < g_num_tempdirs; ++i) {
        recursive_unlink(g_tempdirs[i]);
        free(g_tempdirs[i]);
    }
    free(g_tempdirs);
    g_tempdirs = NULL;
    pthread_mutex_unlock(&tempdir_lock);
}

char *create_tempdir(const char *name, int mode, char *err, size_t err_len)
{
    char *tdir = NULL, tmp[PATH_MAX];
    int nonce, pid;
    const char *base = getenv("TMPDIR");

    err[0] = '\0';
    if (!base) {
        base = "/tmp";
    }
    if (base[0] != '/') {
        // canonicalize non-abosolute TMPDIR
        if (realpath(base, tmp) == NULL) {
            int e = errno;
            snprintf(err, err_len, "realpath(%s) failed: %s",
                     base, terror(e));
            return NULL;
        }
        base = tmp;
    }
    pthread_mutex_lock(&tempdir_lock);
    nonce = g_tempdir_nonce++;
    pthread_mutex_unlock(&tempdir_lock);
    pid = getpid();
    if (asprintf(&tdir, "%s/%s.tmp.%08d.%08d",
             base, name, pid, nonce) < 0) {
        snprintf(err, err_len, "asprintf failed");
        return NULL;
    }
    if (mkdir(tdir, mode) == -1) {
        int e = errno;
        snprintf(err, err_len, "mkdir(%s) failed: %s", tdir, terror(e));
        free(tdir);
        return NULL;
    }
    return tdir;
}

int register_tempdir_for_cleanup(const char *tdir)
{
    char **tempdirs;
    pthread_mutex_lock(&tempdir_lock);
    tempdirs = realloc(g_tempdirs, sizeof(char*) * (g_num_tempdirs + 1));
    if (!tempdirs)
        return -ENOMEM;
    g_tempdirs = tempdirs;
    g_tempdirs[g_num_tempdirs] = strdup(tdir);
    if (g_num_tempdirs == 0)
        atexit(cleanup_registered_tempdirs);
    g_num_tempdirs++;
    pthread_mutex_unlock(&tempdir_lock);
    return 0;
}

void unregister_tempdir_for_cleanup(const char *tdir)
{
    int i;
    char **tempdirs;
    pthread_mutex_lock(&tempdir_lock);
    if (g_num_tempdirs == 0) {
        pthread_mutex_unlock(&tempdir_lock);
        return;
    }
    for (i = 0; i < g_num_tempdirs; ++i) {
        if (strcmp(g_tempdirs[i], tdir) == 0)
            break;
    }
    if (i == g_num_tempdirs) {
        pthread_mutex_unlock(&tempdir_lock);
        return;
    }
    free(g_tempdirs[i]);
    g_tempdirs[i] = g_tempdirs[g_num_tempdirs - 1];
    tempdirs = realloc(g_tempdirs, sizeof(char*) * g_num_tempdirs - 1);
    if (tempdirs) {
        g_tempdirs = tempdirs;
    }
    g_num_tempdirs--;
    pthread_mutex_unlock(&tempdir_lock);
}

static int recursive_unlink_helper(int dirfd, const char *name)
{
    int fd = -1, ret = 0;
    DIR *dfd = NULL;
    struct stat stat;
    struct dirent *de;

    if (dirfd >= 0) {
        fd = openat(dirfd, name, O_RDONLY);
    } else {
        fd = open(name, O_RDONLY);
    }
    if (fd < 0) {
        ret = errno;
        fprintf(stderr, "error opening %s: %s\n", name, terror(ret));
        goto done;
    }
    if (fstat(fd, &stat) < 0) {
        ret = errno;
        fprintf(stderr, "failed to stat %s: %s\n", name, terror(ret));
        goto done;
    }
    if (!(S_ISDIR(stat.st_mode))) {
        if (unlinkat(dirfd, name, 0)) {
            ret = errno;
            fprintf(stderr, "failed to unlink %s: %s\n", name, terror(ret));
            goto done;
        }
    } else {
        dfd = fdopendir(fd);
        if (!dfd) {
            ret = errno;
            fprintf(stderr, "fopendir(%s) failed: %s\n", name, terror(ret));
            goto done;
        }
        while ((de = readdir(dfd))) {
            if (!strcmp(de->d_name, "."))
                continue;
            if (!strcmp(de->d_name, ".."))
                continue;
            ret = recursive_unlink_helper(fd, de->d_name);
            if (ret)
                goto done;
        }
        if (unlinkat(dirfd, name, AT_REMOVEDIR) < 0) {
            ret = errno;
            goto done;
        }
    }
done:
    if (fd >= 0) {
        close(fd);
    }
    if (dfd) {
        closedir(dfd);
    }
    return -ret;
}

int recursive_unlink(const char *path)
{
    return recursive_unlink_helper(-1, path);
}

// vim:ts=4:sw=4:et
