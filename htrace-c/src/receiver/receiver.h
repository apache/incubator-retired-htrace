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

#ifndef APACHE_HTRACE_RECEIVER_RECEIVER_H
#define APACHE_HTRACE_RECEIVER_RECEIVER_H

/**
 * @file rcv.h
 *
 * Functions related to HTrace receivers.
 *
 * This is an internal header, not intended for external use.
 */

struct htrace_conf;
struct htrace_span;
struct htracer;

/**
 * Base class for an HTrace span receiver.
 *
 * Implementations should begin with this class as the first member.
 */
struct htrace_rcv {
    /**
     * The type of the receiver.
     */
    const struct htrace_rcv_ty *ty;
};

/**
 * A table of callbacks that implements an HTrace span receiver.
 */
struct htrace_rcv_ty {
    /**
     * The name of this HTrace span receiver type.
     */
    const char * const name;

    /**
     * Create an HTrace span receiver of this type.
     *
     * @param tracer        The HTrace context to use.  The span receiver may
     *                          hold on to this pointer.
     * @param conf          The HTrace configuration to use.  The span
     *                          receiver must not hold on to this pointer.
     *
     * @return              The HTrace span receciver.
     */
    struct htrace_rcv *(*create)(struct htracer *tracer,
                                 const struct htrace_conf *conf);

    /**
     * Callback to add a new span.
     *
     * @param rcv           The HTrace span receiver.
     * @param span          The trace span to add.
     */
    void (*add_span)(struct htrace_rcv *rcv, struct htrace_span *span);

    /**
     * Flush all buffered spans to the backing store used by this receiver.
     *
     * @param rcv           The HTrace span receiver.
     */
    void (*flush)(struct htrace_rcv *rcv);

    /**
     * Frees this HTrace span receiver.
     *
     * @param rcv           The HTrace span receiver.
     */
    void (*free)(struct htrace_rcv *rcv);
};

/**
 * Create an HTrace span receiver.
 *
 * @param tracer        The HTrace context to use.  The newly created
 *                          span receiver may hold on to this pointer.
 * @param conf          The HTrace configuration to use.  The newly
 *                          created span receiver will not hold on to this
 *                          pointer.
 *
 * @return              The HTrace span receciver.
 */
struct htrace_rcv *htrace_rcv_create(struct htracer *tracer,
                                     const struct htrace_conf *conf);

/*
 * HTrace span receiver types.
 */
const struct htrace_rcv_ty g_noop_rcv_ty;
const struct htrace_rcv_ty g_local_file_rcv_ty;
const struct htrace_rcv_ty g_htraced_rcv_ty;

#endif

// vim: ts=4: sw=4: et
