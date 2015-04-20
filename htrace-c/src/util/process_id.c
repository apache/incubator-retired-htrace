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

#include "util/log.h"
#include "util/process_id.h"

#include <arpa/inet.h>
#include <errno.h>
#include <ifaddrs.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

/**
 * @file process_id.c
 *
 * Implements process IDs for the HTrace C client.
 */

/**
 * The maximum number of bytes that can be in a process ID substitution
 * variable.
 */
#define MAX_VAR_NAME 32

enum ip_addr_type {
    ADDR_TYPE_IPV6_LOOPBACK = 0,
    ADDR_TYPE_IPV4_LOOPBACK,
    ADDR_TYPE_IPV6_OTHER,
    ADDR_TYPE_IPV4_OTHER,
    ADDR_TYPE_IPV6_SITE_LOCAL,
    ADDR_TYPE_IPV4_SITE_LOCAL
};

static int handle_process_subst_var(struct htrace_log *lg, char **out,
                                    const char *var, const char *tname);

enum ip_addr_type get_ipv4_addr_type(const struct sockaddr_in *ip);

enum ip_addr_type get_ipv6_addr_type(const struct sockaddr_in6 *ip);

static int append_char(char **out, int *j, char c)
{
    char *nout = realloc(*out, *j + 2); // leave space for NULL
    if (!nout) {
        return 0;
    }
    *out = nout;
    (*out)[*j] = c;
    *j = *j + 1;
    (*out)[*j] = '\0';
    return 1;
}

char *calculate_process_id(struct htrace_log *lg, const char *fmt,
                           const char *tname)
{
    int i = 0, j = 0, escaping = 0, v = 0;
    char *out = NULL, *var = NULL;

    out = strdup("");
    if (!out) {
        goto oom;
    }
    while (1) {
        char c = fmt[i++];
        if (c == '\0') {
            break;
        } else if (c == '\\') {
            if (!escaping) {
                escaping = 1;
                continue;
            }
        }
        switch (v) {
        case 0:
            if (c == '%') {
                if (!escaping) {
                    if (!append_char(&var, &v, '%')) {
                        goto oom;
                    }
                    continue;
                }
            }
            break;
        case 1:
            if (c == '{') {
                if (!escaping) {
                    if (!append_char(&var, &v, '{')) {
                        goto oom;
                    }
                    continue;
                }
            }
            if (!append_char(&out, &j, '%')) {
                goto oom;
            }
            break;
        default:
            if (c == '}') {
                if (!escaping) {
                    if (!append_char(&var, &v, '}')) {
                        goto oom;
                    }
                    var[v++] = '\0';
                    if (!handle_process_subst_var(lg, &out, var, tname)) {
                        goto oom;
                    }
                    free(var);
                    var = NULL;
                    j = strlen(out);
                    v = 0;
                    continue;
                }
            }
            escaping = 0;
            if (!append_char(&var, &v, c)) {
                goto oom;
            }
            continue;
        }
        escaping = 0;
        v = 0;
        if (!append_char(&out, &j, c)) {
            goto oom;
        }
    }
    out[j] = '\0';
    if (v > 0) {
      htrace_log(lg, "calculate_process_id(%s): unterminated process ID "
                 "substitution variable at the end of the format string.",
                 fmt);
    }
    free(var);
    return out;

oom:
    htrace_log(lg, "calculate_process_id(tname=%s): OOM\n", tname);
    free(out);
    free(var);
    return NULL;
}

static int handle_process_subst_var(struct htrace_log *lg, char **out,
                                    const char *var, const char *tname)
{
    char *nout = NULL;

    if (strcmp(var, "%{tname}") == 0) {
        if (asprintf(&nout, "%s%s", *out, tname) < 0) {
            htrace_log(lg, "handle_process_subst_var(var=%s): OOM", var);
            return 0;
        }
        free(*out);
        *out = nout;
    } else if (strcmp(var, "%{ip}") == 0) {
        char ip_str[256];
        get_best_ip(lg, ip_str, sizeof(ip_str));
        if (asprintf(&nout, "%s%s", *out, ip_str) < 0) {
            htrace_log(lg, "handle_process_subst_var(var=%s): OOM", var);
            return 0;
        }
        free(*out);
        *out = nout;
    } else if (strcmp(var, "%{pid}") == 0) {
        char pid_str[64];
        pid_t pid = getpid();

        snprintf(pid_str, sizeof(pid_str), "%lld", (long long)pid);
        if (asprintf(&nout, "%s%s", *out, pid_str) < 0) {
            htrace_log(lg, "handle_process_subst_var(var=%s): OOM", var);
            return 0;
        }
        free(*out);
        *out = nout;
    } else {
        htrace_log(lg, "handle_process_subst_var(var=%s): unknown process "
                   "ID substitution variable.\n", var);
    }
    return 1;
}

/**
 * Get the "best" IP address for this node.
 *
 * This is complicated since nodes can have multiple network interfaces,
 * and each network interface can have multiple IP addresses.  What we're
 * looking for here is an IP address that will serve to identify this node
 * to HTrace.  So we prefer site-local addresess (i.e. private ones on the
 * LAN) to publicly routable interfaces.  If there are multiple addresses
 * to choose from, we select the one which comes first in textual sort
 * order.  This should ensure that we at least consistently call each node
 * by a single name.
 */
void get_best_ip(struct htrace_log *lg, char *ip_str, size_t ip_str_len)
{
    struct ifaddrs *head, *ifa;
    enum ip_addr_type ty = ADDR_TYPE_IPV4_LOOPBACK, nty;
    char temp_ip_str[128];

    snprintf(ip_str, ip_str_len, "%s", "127.0.0.1");
    if (getifaddrs(&head) < 0) {
        int res = errno;
        htrace_log(lg, "get_best_ip: getifaddrs failed: %s\n", terror(res));
        return;
    }
    for (ifa = head; ifa; ifa = ifa->ifa_next){
        if (!ifa->ifa_addr) {
            continue;
        }
        if (ifa->ifa_addr->sa_family == AF_INET) {
            struct sockaddr_in *addr =
                (struct sockaddr_in *)ifa->ifa_addr;
            nty = get_ipv4_addr_type(addr);
            if (nty < ty) {
                continue;
            }
            if (!inet_ntop(AF_INET, &addr->sin_addr, temp_ip_str,
                           sizeof(temp_ip_str))) {
                htrace_log(lg, "get_best_ip_impl: inet_ntop(%s, AF_INET) "
                           "failed\n", ifa->ifa_name);
                continue;
            }
            if ((nty == ty) && (strcmp(temp_ip_str, ip_str) > 0)) {
                continue;
            }
            snprintf(ip_str, ip_str_len, "%s", temp_ip_str);
            ty = nty;
        } else if (ifa->ifa_addr->sa_family == AF_INET6) {
            struct sockaddr_in6 *addr =
                (struct sockaddr_in6 *)ifa->ifa_addr;
            nty = get_ipv6_addr_type(addr);
            if (nty < ty) {
                continue;
            }
            if (!inet_ntop(AF_INET6, &addr->sin6_addr, temp_ip_str,
                           sizeof(temp_ip_str))) {
                htrace_log(lg, "get_best_ip_impl: inet_ntop(%s, AF_INET6) "
                           "failed\n", ifa->ifa_name);
                continue;
            }
            if ((nty == ty) && (strcmp(temp_ip_str, ip_str) > 0)) {
                continue;
            }
            snprintf(ip_str, ip_str_len, "%s", temp_ip_str);
            ty = nty;
        }
    }
    freeifaddrs(head);
}

enum ip_addr_type get_ipv4_addr_type(const struct sockaddr_in *ip)
{
    union {
        uint8_t b[4];
        uint32_t addr;
    } addr;
    addr.addr = ip->sin_addr.s_addr; // always big-endian
    if (addr.b[0] == 127) { // 127.0.0.0/24
        return ADDR_TYPE_IPV4_LOOPBACK;
    }
    if ((addr.b[0] == 10) && (addr.b[1] == 0) && (addr.b[2] == 0)) {
        return ADDR_TYPE_IPV4_SITE_LOCAL; // 10.0.0.0/8
    }
    if ((addr.b[0] == 192) && (addr.b[1] == 168)) {
        return ADDR_TYPE_IPV4_SITE_LOCAL; // 192.168.0.0/16
    }
    if ((addr.b[0] == 172) && (addr.b[1] == 16) && ((addr.b[2] & 0xf0) == 0)) {
        return ADDR_TYPE_IPV4_SITE_LOCAL; // 172.16.0.0/12
    }
    return ADDR_TYPE_IPV4_OTHER;
}

enum ip_addr_type get_ipv6_addr_type(const struct sockaddr_in6 *ip)
{
    if (IN6_IS_ADDR_LOOPBACK(ip)) {
        return ADDR_TYPE_IPV6_LOOPBACK;
    } else if (IN6_IS_ADDR_SITELOCAL(ip)) {
        return ADDR_TYPE_IPV6_SITE_LOCAL;
    } else {
        return ADDR_TYPE_IPV6_OTHER;
    }
}

// vim:ts=4:sw=4:et
