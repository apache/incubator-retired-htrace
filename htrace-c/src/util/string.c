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
#include "util/string.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int fwdprintf(char **buf, int* rem, const char *fmt, ...)
{
    int amt, res;
    char *b;
    va_list ap;

    if (!buf) {
        char tmp[1] = { 0 };
        va_start(ap, fmt);
        res = vsnprintf(tmp, sizeof(tmp), fmt, ap);
        va_end(ap);
        if (res < 0) {
            res = 0;
        }
        return res;
    }
    b = *buf;
    va_start(ap, fmt);
    amt = *rem;
    res = vsnprintf(b, amt, fmt, ap);
    va_end(ap);
    if (res < 0) {
        res = 0;
    } else {
        int sub = (amt < res) ? amt : res;
        *rem = amt - sub;
        *buf = b + sub;
    }
    return res;
}

int validate_json_string(struct htrace_log *lg, const char *str)
{
    const unsigned char *b = (const unsigned char *)str;
    int off = 0;

    while(*b) {
        // Note: we don't allow newline (0x0a), tab (0x09), or carriage return
        // (0x0d) because they cause problems down the line.
        if (((0x20 <= b[0] && b[0] <= 0x7E)) &&
                ((b[0] != '"') && (b[0] != '\\'))) {
            b++;
            off++;
            continue;
        }
        if((0xC2 <= b[0] && b[0] <= 0xDF) && (0x80 <= b[1] && b[1] <= 0xBF)) {
            b += 2; // 2-byte UTF-8, U+0080 to U+07FF
            off += 2;
            continue;
        }
        if ((b[0] == 0xe0 &&
                    (0xa0 <= b[1] && b[1] <= 0xbf) &&
                    (0x80 <= b[2] && b[2] <= 0xbf)
                ) || (
                    ((0xe1 <= b[0] && b[0] <= 0xec) ||
                        b[0] == 0xee ||
                        b[0] == 0xef) &&
                    (0x80 <= b[1] && b[1] <= 0xbf) &&
                    (0x80 <= b[2] && b[2] <= 0xbf)
                ) || (
                    b[0] == 0xed &&
                    (0x80 <= b[1] && b[1] <= 0x9f) &&
                    (0x80 <= b[2] && b[2] <= 0xbf)
                )) {
            b += 3; // 3-byte UTF-8, U+0800 U+FFFF
            off += 3;
            continue;
        }
        // Note: we don't allow code points outside the basic multilingual plane
        // (BMP) at the moment.  The problem with them is that Javascript
        // doesn't support them directly (they have to be encoded with UCS-2
        // surrogate pairs).  TODO: teach htraced to do that encoding.
        if (lg) {
            htrace_log(lg, "validate_json_string(%s): byte %d (0x%02x) "
                       "was problematic.\n", str, off, b[0]);
        }
        return 0;
    }
    return 1;
}

int parse_endpoint(struct htrace_log *lg, const char *endpoint,
                   int default_port, char **remote_out, int *port)
{
    const char *remotestr;
    const char *portstr;
    char *remote = NULL;
    int remote_len;

    if (endpoint[0] == '[') {
        remotestr = endpoint + 1;
        remote_len = strcspn(remotestr, "]");
        if (remotestr[remote_len] != ']') {
            htrace_log(lg, "parse_hostport: found open square bracket, but "
                       "not matching close square bracket.\n");
            return 0;
        }
        if (remotestr[remote_len + 1] == ':') {
            portstr = remotestr + remote_len + 2;
        } else {
            portstr = NULL;
        }
    } else {
        remotestr = endpoint;
        remote_len = strcspn(remotestr, ":");
        if (remotestr[remote_len] == ':') {
            portstr = remotestr + remote_len + 1;
        } else {
            portstr = NULL;
        }
    }
    remote = malloc(remote_len + 1);
    if (!remote) {
        htrace_log(lg, "parse_hostport: unable to allocate %d-byte string.\n",
                   remote_len);
        return 0;
    }
    memcpy(remote, remotestr, remote_len);
    remote[remote_len] = '\0';
    if (!portstr) {
        *port = default_port;
    } else {
        int p = atoi(portstr);
        if ((p <= 0) || (p > 0xffff)) {
            free(remote);
            htrace_log(lg, "parse_hostport: parse port string '%s'\n",
                       portstr);
            return 0;
        }
        *port = p;
    }
    *remote_out = remote;
    return 1;
}

// vim: ts=4:sw=4:et
