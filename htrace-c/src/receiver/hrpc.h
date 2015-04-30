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

#ifndef APACHE_HTRACE_RECEIVER_HRPC
#define APACHE_HTRACE_RECEIVER_HRPC

/**
 * @file hrpc.h
 *
 * Functions related to HRPC.
 *
 * This is an internal header, not intended for external use.
 */

#include <stdint.h>
#include <unistd.h>

#define METHOD_ID_WRITE_SPANS 0x1

struct htrace_log;

/**
 * Create an HRPC client.
 *
 * @param lg                The log object to use for the HRPC client.
 * @param write_timeo_ms    The TCP write timeout to use.
 * @param read_timeo_ms     The TCP read timeout to use.
 * @param hostpost          The hostname and port, separated by a colon.
 *
 * @param                   NULL on OOM; the hrpc_client otherwise.
 */
struct hrpc_client *hrpc_client_alloc(struct htrace_log *lg,
                uint64_t write_timeo_ms, uint64_t read_timeo_ms,
                const char *endpoint);

/**
 * Free the HRPC client.
 *
 * @param hcli              The HRPC client.
 */
void hrpc_client_free(struct hrpc_client *hcli);

/**
 * Make a blocking call using the HRPC client.
 *
 * @param hcli              The HRPC client.
 * @param method_id         The method ID to use.
 * @param req               The request buffer to send.
 * @param req_len           The size of the request buffer to send.
 * @param err               (out param) Will be set to a malloced
 *                              NULL-terminated string if the server returned an
 *                              error response.  NULL otherwise.
 * @param resp              (out param) The response body.  Will be set to the
 *                              response body if the function returns nonzero.
 * @param resp_len          (out param) The length of the response body.
 *
 * @return                  0 on failure, 1 on success.
 */
int hrpc_client_call(struct hrpc_client *hcli, uint32_t method_id,
                     const void *req, size_t req_len,
                     char **err, void **resp, size_t *resp_len);

/**
 * Get the endpoint for this HRPC client.
 *
 * @param hcli              The HRPC client.
 *
 * @return                  The endpoint.  This string will be valid for the
 *                              lifetime of the HRPC client.
 */
const char *hrpc_client_get_endpoint(struct hrpc_client *hcli);

#endif

// vim: ts=4: sw=4: et
