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

// Get the POSIX definition of strerror_r.
#define _POSIX_C_SOURCE 200112L
#undef _GNU_SOURCE

#include "util/build.h"
#include "util/log.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>

/**
 * @file terror.c
 *
 * Implements the thread-safe terror() function.
 *
 * glibc makes it difficult to get access to the POSIX definition of strerror_r.
 * Keeping this in a separate file allows us to put the proper macro magic at
 * the top of just this file.
 */

#ifdef HAVE_IMPROVED_TLS
const char *terror(int err)
{
    static __thread char buf[4096];
    int ret;

    ret = strerror_r(err, buf, sizeof(buf));
    if (ret) {
        return "unknown error";
    }
    return buf;
}
#else
extern const char *sys_errlist[];
extern int sys_nerr;

const char *terror(int err)
{
    if (err >= sys_nerr) {
        return "unknown error";
    }
    return sys_errlist[err];
}
#endif

// vim: ts=4:sw=4:et
