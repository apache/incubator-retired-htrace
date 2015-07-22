/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "core/conf.h"
#include "core/htrace.h"
#include "test/mini_htraced.h"
#include "test/temp_dir.h"
#include "test/test_config.h"
#include "test/test.h"
#include "util/log.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <json_object.h>
#include <json_tokener.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

/**
 * The maximum size of the notification data sent from the htraced daemon on
 * startup.
 */
#define MAX_NDATA 65536

/**
 * The separator to use in between paths.  TODO: portability
 */
#define PATH_LIST_SEP ':'

/**
 * The maximum number of arguments when launching an external process.
 */
#define MAX_LAUNCH_ARGS 32

/**
 * Retry an operation that may get EINTR.
 * The operation must return a non-negative value on success.
 */
#define RETRY_ON_EINTR(ret, expr) do { \
    ret = expr; \
    if (ret >= 0) \
        break; \
} while (errno == EINTR);

#define MINI_HTRACED_LAUNCH_REDIRECT_FDS 0x1

static void mini_htraced_open_snsock(struct mini_htraced *ht, char *err,
                                     size_t err_len);

static void mini_htraced_write_conf_file(struct mini_htraced *ht,
                                  char *err, size_t err_len);

static int mini_htraced_write_conf_key(FILE *fp, const char *key,
                                       const char *fmt, ...)
    __attribute__((format(printf, 3, 4)));

static void mini_htraced_launch_daemon(struct mini_htraced *ht,
                                char *err, size_t err_len);

/**
 * Launch an external process.
 *
 * @param ht                The mini htraced object.
 * @param path              The binary to launch
 * @param err               (out param) the error, or empty string on success.
 * @param err_len           The length of the error buffer.
 * @param flags             The flags.
 *                              MINI_HTRACED_LAUNCH_REDIRECT_FDS: redirect
 *                                  stderr, stdout, stdin to null.
 *                                  finished successfully.
 * @param ...               Additional arguments to pass to the process.
 *                              NULL_terminated.  The first argument will
 *                              always be the path to the binary.
 *
 * @return                  The new process ID, on success.  -1 on failure.
 *                              The error string will always be set on failure.
 */
pid_t mini_htraced_launch(const struct mini_htraced *ht, const char *path,
                         char *err, size_t err_len, int flags, ...)
    __attribute__((sentinel));


static void mini_htraced_read_startup_notification(struct mini_htraced *ht,
                                       char *err, size_t err_len);

static void parse_startup_notification(struct mini_htraced *ht,
                                       char *ndata, size_t ndata_len,
                                       char *err, size_t err_len);

void mini_htraced_build(const struct mini_htraced_params *params,
                        struct mini_htraced **hret,
                        char *err, size_t err_len)
{
    struct mini_htraced *ht = NULL;
    int i, ret;

    err[0] = '\0';
    ht = calloc(1, sizeof(*ht));
    if (!ht) {
        snprintf(err, err_len, "out of memory allocating mini_htraced object");
        goto done;
    }
    ht->snsock = -1;
    ht->root_dir = create_tempdir(params->name, 0777, err, err_len);
    if (err[0]) {
        goto done;
    }
    ret = register_tempdir_for_cleanup(ht->root_dir);
    if (ret) {
        snprintf(err, err_len, "register_tempdir_for_cleanup(%s) "
                 "failed: %s", ht->root_dir, terror(ret));
        goto done;
    }
    for (i = 0; i < NUM_DATA_DIRS; i++) {
        if (asprintf(ht->data_dir + i, "%s/dir%d", ht->root_dir, i) < 0) {
            ht->data_dir[i] = NULL;
            snprintf(err, err_len, "failed to create path to data dir %d", i);
            goto done;
        }
    }
    if (asprintf(&ht->htraced_log_path, "%s/htraced.log", ht->root_dir) < 0) {
        ht->htraced_log_path = NULL;
        snprintf(err, err_len, "failed to create path to htraced.log");
        goto done;
    }
    if (asprintf(&ht->htraced_conf_path, "%s/htraced-conf.xml",
                 ht->root_dir) < 0) {
        ht->htraced_conf_path = NULL;
        snprintf(err, err_len, "failed to create path to htraced-conf.xml");
        goto done;
    }
    mini_htraced_open_snsock(ht, err, err_len);
    if (err[0]) {
        goto done;
    }
    mini_htraced_write_conf_file(ht, err, err_len);
    if (err[0]) {
        goto done;
    }
    mini_htraced_launch_daemon(ht, err, err_len);
    if (err[0]) {
        goto done;
    }
    mini_htraced_read_startup_notification(ht, err, err_len);
    if (err[0]) {
        goto done;
    }
    if (asprintf(&ht->client_conf_defaults, "%s=%s",
             HTRACED_ADDRESS_KEY, ht->htraced_http_addr) < 0) {
        ht->client_conf_defaults = NULL;
        snprintf(err, err_len, "failed to allocate client conf defaults.");
        goto done;
    }
    *hret = ht;
    err[0] = '\0';

done:
    if (err[0]) {
        mini_htraced_free(ht);
    }
}

static int do_waitpid(pid_t pid, char *err, size_t err_len)
{
    err[0] = '\0';
    while (1) {
        int status, res = waitpid(pid, &status, 0);
        if (res < 0) {
            if (errno == EINTR) {
                continue;
            }
            snprintf(err, err_len, "waitpid(%lld) error: %s",
                    (long long)pid, terror(res));
            return -1;
        }
        if (WIFEXITED(status)) {
            return WEXITSTATUS(status);
        }
        return -1; // signal or other exit
    }
}

void mini_htraced_stop(struct mini_htraced *ht)
{
    char err[512];
    size_t err_len = sizeof(err);

    if (!ht->htraced_pid_valid) {
        return;
    }
    kill(ht->htraced_pid, SIGTERM);
    ht->htraced_pid_valid = 0;
    do_waitpid(ht->htraced_pid, err, err_len);
    if (err[0]) {
        fprintf(stderr, "%s\n", err);
    }
}

void mini_htraced_free(struct mini_htraced *ht)
{
    int i;

    if (!ht) {
        return;
    }
    mini_htraced_stop(ht);
    if (ht->root_dir) {
        unregister_tempdir_for_cleanup(ht->root_dir);
        if (!getenv("SKIP_CLEANUP")) {
            recursive_unlink(ht->root_dir);
        }
    }
    free(ht->root_dir);
    for (i = 0; i < NUM_DATA_DIRS; i++) {
        free(ht->data_dir[i]);
    }
    free(ht->htraced_log_path);
    free(ht->htraced_conf_path);
    free(ht->client_conf_defaults);
    if (ht->snsock >= 0) {
        close(ht->snsock);
        ht->snsock = -1;
    }
    free(ht->htraced_http_addr);
    free(ht);
}

static void mini_htraced_open_snsock(struct mini_htraced *ht, char *err,
                                     size_t err_len)
{
    struct sockaddr_in snaddr;
    socklen_t len = sizeof(snaddr);
    struct timeval tv;

    err[0] = '\0';
    memset(&snaddr, 0, sizeof(snaddr));
    snaddr.sin_family = AF_INET;
    snaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    ht->snsock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (ht->snsock < 0) {
        int res = errno;
        snprintf(err, err_len, "Failed to create new socket: %s\n",
                 terror(res));
        return;
    }
    if (bind(ht->snsock, (struct sockaddr *) &snaddr, sizeof(snaddr)) < 0) {
        int res = errno;
        snprintf(err, err_len, "bind failed: %s\n", terror(res));
        return;
    }
    if (getsockname(ht->snsock, (struct sockaddr *)&snaddr, &len) < 0) {
        int res = errno;
        snprintf(err, err_len, "getsockname failed: %s\n", terror(res));
        return;
    }
    ht->snport = ntohs(snaddr.sin_port);
    if (listen(ht->snsock, 32) < 0) {
        int res = errno;
        snprintf(err, err_len, "listen failed: %s\n", terror(res));
        return;
    }
    // On Linux, at least, this makes accept() time out after 30 seconds.  I'm
    // too lazy to use select() here.
    tv.tv_sec = 30;
    tv.tv_usec = 0;
    setsockopt(ht->snsock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}

static void mini_htraced_write_conf_file(struct mini_htraced *ht,
                                  char *err, size_t err_len)
{
    FILE *fp;
    int res;

    err[0] = '\0';
    fp = fopen(ht->htraced_conf_path, "w");
    if (!fp) {
        res = errno;
        snprintf(err, err_len, "fopen(%s) failed: %s",
                 ht->htraced_conf_path, terror(res));
        goto error;
    }
    if (fprintf(fp, "\
<?xml version=\"1.0\"?>\n\
<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n\
<configuration>\n") < 0) {
        goto ioerror;
    }
    if (mini_htraced_write_conf_key(fp, "log.path", "%s",
                                    ht->htraced_log_path)) {
        goto ioerror;
    }
    if (mini_htraced_write_conf_key(fp, "web.address", "127.0.0.1:0")) {
        goto ioerror;
    }
    if (mini_htraced_write_conf_key(fp, "hrpc.address", "127.0.0.1:0")) {
        goto ioerror;
    }
    if (mini_htraced_write_conf_key(fp, "data.store.directories",
            "%s%c%s", ht->data_dir[0], PATH_LIST_SEP, ht->data_dir[1])) {
        goto ioerror;
    }
    if (mini_htraced_write_conf_key(fp, "startup.notification.address",
            "localhost:%d", ht->snport)) {
        goto ioerror;
    }
    if (mini_htraced_write_conf_key(fp, "log.level", "%s", "TRACE")) {
        goto ioerror;
    }
    if (fprintf(fp, "</configuration>\n") < 0) {
        goto ioerror;
    }
    res = fclose(fp);
    if (res) {
        snprintf(err, err_len, "fclose(%s) failed: %s",
                 ht->htraced_conf_path, terror(res));
    }
    return;

ioerror:
    snprintf(err, err_len, "fprintf(%s) error",
             ht->htraced_conf_path);
error:
    if (fp) {
        fclose(fp);
    }
}

static int mini_htraced_write_conf_key(FILE *fp, const char *key,
                                       const char *fmt, ...)
{
    va_list ap;

    if (fprintf(fp, "  <property>\n    <name>%s</name>\n    <value>",
                key) < 0) {
        return 1;
    }
    va_start(ap, fmt);
    if (vfprintf(fp, fmt, ap) < 0) {
        va_end(ap);
        return 1;
    }
    va_end(ap);
    if (fprintf(fp, "</value>\n  </property>\n") < 0) {
        return 1;
    }
    return 0;
}

pid_t mini_htraced_launch(const struct mini_htraced *ht, const char *path,
                         char *err, size_t err_len, int flags, ...)
{
    pid_t pid;
    int res, num_args = 0;
    va_list ap;
    const char *args[MAX_LAUNCH_ARGS + 1];
    const char * const env[] = {
        "HTRACED_CONF_DIR=.", "HTRACED_WEB_DIR=", NULL
    };

    err[0] = '\0';
    if (access(path, X_OK) < 0) {
        snprintf(err, err_len, "The %s binary is not accessible and "
                 "executable.", path);
        return -1;
    }
    va_start(ap, flags);
    args[num_args++] = path;
    while (1) {
        const char *arg = va_arg(ap, char*);
        if (!arg) {
            break;
        }
        if (num_args >= MAX_LAUNCH_ARGS) {
            va_end(ap);
            snprintf(err, err_len, "Too many arguments to launch!  The "
                 "maximum number of arguments is %d.\n", MAX_LAUNCH_ARGS);
            return -1;
        }
        args[num_args++] = arg;
    }
    va_end(ap);
    args[num_args++] = NULL;

    pid = fork();
    if (pid == -1) {
        // Fork failed.
        res = errno;
        snprintf(err, err_len, "fork() failed for %s: %s\n",
                 path, terror(res));
        return -1;
    } else if (pid == 0) {
        // Child process.
        // We don't want to delete the temporary directory when this child
        // process exists.  The parent process is responsible for that, if it
        // is to be done at all.
        unregister_tempdir_for_cleanup(ht->root_dir);

        // Make things nicer by exiting when the parent process exits.
        prctl(PR_SET_PDEATHSIG, SIGHUP);

        if (flags & MINI_HTRACED_LAUNCH_REDIRECT_FDS) {
            int null_fd;
            RETRY_ON_EINTR(null_fd, open("/dev/null", O_WRONLY));
            if (null_fd < 0) {
                _exit(127);
            }
            RETRY_ON_EINTR(res, dup2(null_fd, STDOUT_FILENO));
            if (res < 0) {
                _exit(127);
            }
            RETRY_ON_EINTR(res, dup2(null_fd, STDERR_FILENO));
            if (res < 0) {
                _exit(127);
            }
        }
        if (chdir(ht->root_dir) < 0) {
            _exit(127);
        }
        execve(path, (char *const*)args, (char * const*)env);
        _exit(127);
    }
    // Parent process.
    return pid;
}

static void mini_htraced_launch_daemon(struct mini_htraced *ht,
                                char *err, size_t err_len)
{
    int flags = 0;
    pid_t pid;

    if (!getenv("SKIP_CLEANUP")) {
        flags |= MINI_HTRACED_LAUNCH_REDIRECT_FDS;
    }
    pid = mini_htraced_launch(ht, HTRACED_ABSPATH, err, err_len, flags, NULL);
    if (err[0]) {
        return;
    }
    ht->htraced_pid_valid = 1;
    ht->htraced_pid = pid;
}

void mini_htraced_dump_spans(struct mini_htraced *ht,
                             char *err, size_t err_len,
                             const char *path)
{
    pid_t pid;
    int ret;
    char *addr = NULL, *log_path = NULL;

    err[0] = '\0';
    if (asprintf(&addr, "--addr=%s", ht->htraced_http_addr) < 0) {
        addr = NULL;
        snprintf(err, err_len, "OOM while allocating the addr string");
        return;
    }
    if (asprintf(&log_path, "--Dlog.path=%s/htrace.%05"PRId64".log",
                 ht->root_dir, ht->num_htrace_commands_run) < 0) {
        log_path = NULL;
        snprintf(err, err_len, "OOM while allocating the addr string");
        free(addr);
        return;
    }
    ht->num_htrace_commands_run++;
    pid = mini_htraced_launch(ht, HTRACE_ABSPATH, err, err_len, 0,
                addr, log_path, "dumpAll", path, NULL);
    free(addr);
    free(log_path);
    if (err[0]) {
        return;
    }
    ret = do_waitpid(pid, err, err_len);
    if (err[0]) {
        return;
    }
    if (ret != EXIT_SUCCESS) {
        snprintf(err, err_len, "%s returned non-zero exit status %d\n",
                 HTRACE_ABSPATH, ret);
        return;
    }
}

static void mini_htraced_read_startup_notification(struct mini_htraced *ht,
                                       char *err, size_t err_len)
{
    char *ndata = NULL;
    int res, sock = -1;
    size_t ndata_len = 0;

    err[0] = '\0';
    ndata = malloc(MAX_NDATA);
    if (!ndata) {
        snprintf(err, err_len, "failed to allocate %d byte buffer for "
                 "notification data.", MAX_NDATA);
        goto done;
    }
    RETRY_ON_EINTR(sock, accept(ht->snsock, NULL, NULL));
    if (sock < 0) {
        int e = errno;
        snprintf(err, err_len, "accept failed: %s", terror(e));
        goto done;
    }
    while (ndata_len < MAX_NDATA) {
        res = recv(sock, ndata + ndata_len, MAX_NDATA - ndata_len, 0);
        if (res == 0) {
            break;
        }
        if (res < 0) {
            int e = errno;
            if (e == EINTR) {
                continue;
            }
            snprintf(err, err_len, "recv error: %s", terror(e));
            goto done;
        }
        ndata_len += res;
    }
    parse_startup_notification(ht, ndata, ndata_len, err, err_len);
    if (err[0]) {
        goto done;
    }

done:
    if (sock >= 0) {
        close(sock);
    }
    free(ndata);
}

static void parse_startup_notification(struct mini_htraced *ht,
                                       char *ndata, size_t ndata_len,
                                       char *err, size_t err_len)
{
    struct json_tokener *tok = NULL;
    struct json_object *root = NULL, *http_addr, *process_id, *hrpc_addr;
    int32_t pid;

    err[0] = '\0';
    tok = json_tokener_new();
    if (!tok) {
        snprintf(err, err_len, "json_tokener_new failed.");
        goto done;
    }
    root = json_tokener_parse_ex(tok, ndata, ndata_len);
    if (!root) {
        enum json_tokener_error jerr = json_tokener_get_error(tok);
        snprintf(err, err_len, "Failed to parse startup notification: %s.",
                 json_tokener_error_desc(jerr));
        goto done;
    }
    // Find the http address, in the form of hostname:port, which the htraced
    // is listening on.
    if (!json_object_object_get_ex(root, "HttpAddr", &http_addr)) {
        snprintf(err, err_len, "Failed to find HttpAddr in the startup "
                 "notification.");
        goto done;
    }
    ht->htraced_http_addr = strdup(json_object_get_string(http_addr));
    if (!ht->htraced_http_addr) {
        snprintf(err, err_len, "OOM");
        goto done;
    }
    // Find the HRPC address, in the form of hostname:port, which the htraced
    // is listening on.
    if (!json_object_object_get_ex(root, "HrpcAddr", &hrpc_addr)) {
        snprintf(err, err_len, "Failed to find HrpcAddr in the startup "
                 "notification.");
        goto done;
    }
    ht->htraced_hrpc_addr = strdup(json_object_get_string(hrpc_addr));
    if (!ht->htraced_hrpc_addr) {
        snprintf(err, err_len, "OOM");
        goto done;
    }
    // Check that the process ID from the startup notification matches the
    // process ID from the fork.
    if (!json_object_object_get_ex(root, "ProcessId", &process_id)) {
        snprintf(err, err_len, "Failed to find ProcessId in the startup "
                 "notification.");
        goto done;
    }
    pid = json_object_get_int(process_id);
    if (pid != ht->htraced_pid) {
        snprintf(err, err_len, "Startup notification pid was %lld, but the "
                 "htraced process id was %lld.",
                 (long long)pid, (long long)ht->htraced_pid);
        goto done;
    }

done:
    json_tokener_free(tok);
    if (root) {
        json_object_put(root);
    }
}

// vim: ts=4:sw=4:tw=79:et
