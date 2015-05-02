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

#include "receiver/hrpc.h"
#include "util/log.h"
#include "util/string.h"
#include "util/time.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#if defined(__OpenBSD__)
#include <sys/types.h>
#define be16toh(x) betoh16(x)
#define be32toh(x) betoh32(x)
#define be64toh(x) betoh64(x)
#elif defined(__NetBSD__) || defined(__FreeBSD__)
#include <sys/endian.h>
#else
#include <endian.h>
#endif

/**
 * @file hrpc.c
 *
 * Implements sending messages via HRPC.
 */

#define HRPC_MAGIC 0x43525448U

#define MAX_HRPC_ERROR_LENGTH (4 * 1024 * 1024)

#define MAX_HRPC_BODY_LENGTH (64 * 1024 * 1024)

#define DEFAULT_HTRACED_HRPC_PORT 9075

#define ADDR_STR_MAX (2 + INET6_ADDRSTRLEN + sizeof(":65536"))

struct hrpc_client {
    /**
     * The HTrace log object.
     */
    struct htrace_log *lg;

    /**
     * The tcp write timeout in milliseconds.
     */
    uint64_t write_timeo_ms;

    /**
     * The tcp read timeout in milliseconds.
     */
    uint64_t read_timeo_ms;

    /**
     * The hostname or IP address.  Malloced.
     */
    char *host;

    /**
     * The port.
     */
    int port;

    /**
     * The host:port string.  Malloced.
     */
    char *endpoint;

    /**
     * Socket of current open connection, or -1 if there is no currently open
     * connection.
     */
    int sock;

    /**
     * The sequence number on the connection.
     */
    uint64_t seq;

    /**
     * The remote IP address.
     */
    char addr_str[ADDR_STR_MAX];
};

struct hrpc_req_header {
    uint32_t magic;
    uint32_t method_id;
    uint64_t seq;
    uint32_t length;
} __attribute__((packed,aligned(4)));

struct hrpc_resp_header {
    uint64_t seq;
    uint32_t method_id;
    uint32_t err_length;
    uint32_t length;
} __attribute__((packed,aligned(4)));


static int hrpc_client_open_conn(struct hrpc_client *hcli);
static int try_connect(struct hrpc_client *hcli, struct addrinfo *p);
static int set_socket_read_and_write_timeout(struct hrpc_client *hcli,
                                             int sock);
static int hrpc_client_send_req(struct hrpc_client *hcli, uint32_t method_id,
                    const void *buf1, size_t buf1_len,
                    const void *buf2, size_t buf2_len, uint64_t *seq);
static int hrpc_client_rcv_resp(struct hrpc_client *hcli, uint32_t method_id,
                       uint64_t seq, char **err, void **resp,
                       size_t *resp_len);

struct hrpc_client *hrpc_client_alloc(struct htrace_log *lg,
                uint64_t write_timeo_ms, uint64_t read_timeo_ms,
                const char *endpoint)
{
    struct hrpc_client *hcli;

    hcli = calloc(1, sizeof(*hcli));
    if (!hcli) {
        htrace_log(lg, "Failed to allocate memory for the HRPC client.\n");
        goto error;
    }
    hcli->lg = lg;
    hcli->write_timeo_ms = write_timeo_ms;
    hcli->read_timeo_ms = read_timeo_ms;
    hcli->sock = -1;
    hcli->endpoint = strdup(endpoint);
    if (!hcli->endpoint) {
        htrace_log(lg, "Failed to allocate memory for the endpoint string.\n");
        goto error;
    }
    if (!parse_endpoint(lg, endpoint, DEFAULT_HTRACED_HRPC_PORT,
                   &hcli->host, &hcli->port)) {
        goto error;
    }
    return hcli;

error:
    if (hcli) {
        free(hcli->host);
        free(hcli->endpoint);
        free(hcli);
    }
    return NULL;
}

void hrpc_client_free(struct hrpc_client *hcli)
{
    if (!hcli) {
        return;
    }
    if (hcli->sock >= 0) {
        close(hcli->sock);
        hcli->sock = -1;
    }
    free(hcli->host);
    free(hcli->endpoint);
    free(hcli);
}

int hrpc_client_call(struct hrpc_client *hcli, uint32_t method_id,
                    const void *buf1, size_t buf1_len,
                    const void *buf2, size_t buf2_len,
                    char **err, void **resp, size_t *resp_len)
{
    uint64_t seq;

    if (hcli->sock < 0) {
        if (!hrpc_client_open_conn(hcli)) {
            goto error;
        }
        htrace_log(hcli->lg, "hrpc_client_call: successfully opened connection\n");
    } else {
        htrace_log(hcli->lg, "hrpc_client_call: connection was already open\n");
    }
    if (!hrpc_client_send_req(hcli, method_id,
                              buf1, buf1_len, buf2, buf2_len, &seq)) {
        goto error;
    }
    htrace_log(hcli->lg, "hrpc_client_call: waiting for response\n");
    if (!hrpc_client_rcv_resp(hcli, method_id, seq, err, resp, resp_len)) {
        goto error;
    }
    return 1;

error:
    if (hcli->sock >= 0) {
        close(hcli->sock);
        hcli->sock = -1;
    }
    return 0;
}

static int hrpc_client_open_conn(struct hrpc_client *hcli)
{
    int res, sock = -1;
    struct addrinfo hints, *list, *info;

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    res = getaddrinfo(hcli->host, NULL, &hints, &list);
    if (res) {
        htrace_log(hcli->lg, "hrpc_client_open_conn: "
                   "getaddrinfo(%s) error %d: %s\n",
                   hcli->host, res, gai_strerror(res));
        return 0;
    }
    for (info = list; info; info = info->ai_next) {
        sock = try_connect(hcli, info);
        if (sock >= 0) {
            break;
        }
    }
    freeaddrinfo(list);
    if (!info) {
        htrace_log(hcli->lg, "hrpc_client_open_conn(%s): failed to connect.\n",
                   hcli->host);
        return 0;
    }
    hcli->sock = sock;
    return 1;
}

static int set_port(struct hrpc_client *hcli, struct sockaddr *addr,
                    int ai_family)
{
    switch (ai_family) {
    case AF_INET: {
        struct sockaddr_in *in4 = (struct sockaddr_in*)addr;
        in4->sin_port = htons(hcli->port);
        return 1;
    }
    case AF_INET6: {
        struct sockaddr_in6 *in6 = (struct sockaddr_in6*)addr;
        in6->sin6_port = htons(hcli->port);
        return 1;
    }
    default:
        htrace_log(hcli->lg, "try_connect(%s): set_port %d failed: unknown "
                   "ai_family %d\n", hcli->addr_str, hcli->port, ai_family);
        return 0;
    }
}

static int try_connect(struct hrpc_client *hcli, struct addrinfo *p)
{
    int e, sock = -1;
    char ip[INET6_ADDRSTRLEN];

    e = getnameinfo(p->ai_addr, p->ai_addrlen,
                ip, sizeof(ip), 0, 0, NI_NUMERICHOST);
    if (e) {
        htrace_log(hcli->lg, "try_connect: getnameinfo failed.  error "
                   "%d: %s\n", e, gai_strerror(e));
        return 0;
    }
    snprintf(hcli->addr_str, ADDR_STR_MAX, "%s:%d", ip, hcli->port);
    if (!set_port(hcli, p->ai_addr, p->ai_family)) {
        goto error;
    }
    sock = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (sock < 0) {
        e = errno;
        htrace_log(hcli->lg, "try_connect(%s): failed to create new "
                   "socket: error %d (%s)\n", hcli->addr_str, e, terror(e));
        goto error;
    }
    if (fcntl(sock, F_SETFD, FD_CLOEXEC) < 0) {
        e = errno;
        htrace_log(hcli->lg, "try_connect(%s): fcntl(FD_CLOEXEC) "
                   "failed: error %d (%s)\n", hcli->addr_str, e, terror(e));
        goto error;
    }
    if (!set_socket_read_and_write_timeout(hcli, sock)) {
        goto error;
    }
    if (connect(sock, p->ai_addr, p->ai_addrlen) < 0) {
        e = errno;
        htrace_log(hcli->lg, "try_connect(%s): connect "
                   "failed: error %d (%s)\n", hcli->addr_str, e, terror(e));
        goto error;
    }
    return sock;

error:
    if (sock >= 0) {
        close(sock);
    }
    return -1;
}

static int set_socket_read_and_write_timeout(struct hrpc_client *hcli,
                                             int sock)
{
    struct timeval tv;

    ms_to_timeval(hcli->read_timeo_ms, &tv);
    if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        int e = errno;
        htrace_log(hcli->lg, "setsockopt(%d, SO_RCVTIMEO, %"PRId64") failed: "
                  "error %d (%s)\n", sock, hcli->read_timeo_ms, e, terror(e));
        return 0;
    }

    ms_to_timeval(hcli->write_timeo_ms, &tv);
    if (setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0) {
        int e = errno;
        htrace_log(hcli->lg, "setsockopt(%d, SO_SNDTIMEO, %"PRId64") failed: "
                  "error %d (%s)\n", sock, hcli->write_timeo_ms, e, terror(e));
        return 0;
    }
    return 1;
}

static int hrpc_client_send_req(struct hrpc_client *hcli, uint32_t method_id,
                    const void *buf1, size_t buf1_len,
                    const void *buf2, size_t buf2_len, uint64_t *seq)
{
    // We use writev (scatter/gather I/O) here in order to avoid sending
    // multiple packets when TCP_NODELAY is turned on.
    struct hrpc_req_header hdr;
    struct iovec iov[3];

    hdr.magic = htole64(HRPC_MAGIC);
    hdr.method_id = htole32(method_id);
    *seq = hcli->seq++;
    hdr.seq = htole64(*seq);
    hdr.length = htole32(buf1_len + buf2_len);
    iov[0].iov_base = &hdr;
    iov[0].iov_len = sizeof(hdr);
    iov[1].iov_base = (void*)buf1;
    iov[1].iov_len = buf1_len;
    iov[2].iov_base = (void*)buf2;
    iov[2].iov_len = buf2_len;

    while (1) {
        ssize_t res = writev(hcli->sock, iov, sizeof(iov)/sizeof(iov[0]));
        int i;
        if (res < 0) {
            int e = errno;
            if (e == EINTR) {
                continue;
            }
            htrace_log(hcli->lg, "hrpc_client_send_req: writev error: "
                       "error %d: %s\n", e, terror(e));
            return 0;
        }
        i = 0;
        while (res > 0) {
            if (iov[i].iov_len < res) {
                res -= iov[i].iov_len;
                iov[i].iov_len = 0;
            } else {
                iov[i].iov_len -= res;
                res = 0;
            }
            if (++i >= (sizeof(iov)/sizeof(iov[0]))) {
                if (res == 0) {
                    return 1;
                }
                htrace_log(hcli->lg, "hrpc_client_send_req: unexpectedly "
                           "large writev return.\n");
                return 0;
            }
        }
    }
}

static int safe_read(int fd, void *buf, size_t amt)
{
    uint8_t *b = buf;
    int e, res, nread = 0;

    while (1) {
        res = read(fd, b + nread, amt - nread);
        if (res <= 0) {
            if (res == 0) {
                return nread;
            }
            e = errno;
            if (e == EINTR) {
                continue;
            }
            return -e;
        }
        nread += res;
        if (nread >= amt) {
            return nread;
        }
    }
}

static int hrpc_client_rcv_resp(struct hrpc_client *hcli, uint32_t method_id,
                                uint64_t seq, char **err_out, void **resp_out,
                                size_t *resp_len)
{
    int res;
    struct hrpc_resp_header hdr;
    uint64_t resp_seq;
    uint32_t resp_method_id, err_length, length;
    char *err = NULL, *resp = NULL;

    res = safe_read(hcli->sock, &hdr, sizeof(hdr));
    if (res < 0) {
        htrace_log(hcli->lg, "hrpc_client_rcv_resp(%s): error reading "
                   "response header: %d (%s)\n", hcli->addr_str, -res,
                   terror(-res));
        goto error;
    }
    if (res != sizeof(hdr)) {
        htrace_log(hcli->lg, "hrpc_client_rcv_resp(%s): unexpected EOF "
                   "reading response header.\n", hcli->addr_str);
        goto error;
    }
    resp_seq = le64toh(hdr.seq);
    if (resp_seq != seq) {
        htrace_log(hcli->lg, "hrpc_client_rcv_resp(%s): expected sequence "
                   "ID 0x%"PRIx64", but got sequence ID 0x%"PRId64".\n",
                   hcli->addr_str, seq, resp_seq);
        goto error;
    }
    resp_method_id = le32toh(hdr.method_id);
    if (resp_method_id != method_id) {
        htrace_log(hcli->lg, "hrpc_client_rcv_resp(%s): expected method "
                   "ID 0x%"PRIx32", but got method ID 0x%"PRId32".\n",
                   hcli->addr_str, method_id, resp_method_id);
        goto error;
    }
    err_length = le32toh(hdr.err_length);
    if (err_length > MAX_HRPC_ERROR_LENGTH) {
        htrace_log(hcli->lg, "hrpc_client_rcv_resp(%s): error length was "
                   "%"PRId32", but the maximum error length is %"PRId32".",
                   hcli->addr_str, err_length, MAX_HRPC_ERROR_LENGTH);
        goto error;
    }
    if (err_length > 0) {
        err = malloc(err_length + 1);
        res = safe_read(hcli->sock, err, err_length);
        if (res < 0) {
            htrace_log(hcli->lg, "hrpc_client_rcv_resp(%s): error reading "
                       "error string: %d (%s)\n", hcli->addr_str, -res,
                       terror(-res));
            goto error;
        }
        if (res != err_length) {
            htrace_log(hcli->lg, "hrpc_client_rcv_resp(%s): unexpected EOF "
                       "reading error string.\n", hcli->addr_str);
            goto error;
        }
        err[err_length] = '\0';
    }
    length = le32toh(hdr.length);
    if (length > MAX_HRPC_BODY_LENGTH) {
        htrace_log(hcli->lg, "hrpc_client_rcv_resp(%s): body length was "
                   "%"PRId32", but the maximum body length is %"PRId32".",
                   hcli->addr_str, length, MAX_HRPC_BODY_LENGTH);
        goto error;
    }
    if (length > 0) {
        resp = malloc(length);
        res = safe_read(hcli->sock, resp, length);
        if (res < 0) {
            htrace_log(hcli->lg, "hrpc_client_rcv_resp(%s): error reading "
                       "body: %d (%s)\n", hcli->addr_str, -res, terror(-res));
            goto error;
        }
        if (res != length) {
            htrace_log(hcli->lg, "hrpc_client_rcv_resp(%s): unexpected EOF "
                       "reading body.\n", hcli->addr_str);
            goto error;
        }
    }
    *err_out = err;
    *resp_out = resp;
    *resp_len = length;
    return 1;

error:
    free(err);
    free(resp);
    *err_out = NULL;
    *resp_out = NULL;
    *resp_len = 0;
    return 0;
}

const char *hrpc_client_get_endpoint(struct hrpc_client *hcli)
{
    return hcli->endpoint;
}

// vim:ts=4:sw=4:et
