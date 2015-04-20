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

#ifndef APACHE_HTRACE_HTRACE_HPP
#define APACHE_HTRACE_HTRACE_HPP

#include "htrace.h"

#include <string>

/**
 * The public C++ API for the HTrace native client.
 *
 * The C++ API is a wrapper around the C API.  The advantage of this is that we
 * can change the C++ API in this file without breaking binary compatibility.
 *
 * EXCEPTIONS
 * We do not use exceptions in this API.  This code should be usable by
 * libraries and applications that are using the Google C++ coding style.
 * The one case where we do use exceptions is to translate NULL pointer returns
 * on OOM into std::bad_alloc exceptions.  In general, it is extremely unlikely
 * that the size of memory allocations we are doing will produce OOM.  Most
 * C++ programs do not attempt to handle OOM anyway because of the extra code
 * complexity that would be required.  So translating this into an exception is
 * fine.
 *
 * C++11
 * This code should not require C++11.  We might add #ifdefs later to take
 * advantage of certain C++11 or later features if they are available.
 */

namespace htrace {
  class Sampler;
  class Scope;
  class Tracer;

  /**
   * An HTrace Configuration object.
   *
   * Configurations are thread-safe.  They can be used by multiple threads
   * simultaneously.
   */
  class Conf {
  public:
    /**
     * Create a new HTrace Conf.
     *
     * @param values    A configuration string containing a series of
     *                  semicolon-separated key=value entries.
     *                  We do not hold on to a reference to this string.
     * @param defaults  Another semicolon-separated set of key=value entries.
     *                  The defaults to be used when there is no corresponding
     *                  value in 'values.' We do not hold on to a reference to
     *                  this string.
     */
    Conf(const char *values)
      : conf_(htrace_conf_from_str(values))
    {
      if (!conf_) {
        throw std::bad_alloc();
      }
    }

    Conf(const std::string &values)
      : conf_(htrace_conf_from_str(values.c_str()))
    {
      if (!conf_) {
        throw std::bad_alloc();
      }
    }

    ~Conf() {
      htrace_conf_free(conf_);
      conf_ = NULL;
    }

  private:
    friend class Tracer;
    friend class Sampler;
    Conf &operator=(Conf &other); // Can't copy
    Conf(Conf &other);
    struct htrace_conf *conf_;
  };

  /**
   * An HTrace context object.
   *
   * Contexts are thread-safe.  They can be used by multiple threads simultaneoy
   * Most applications will not need more than one HTrace context, which is
   * often global (or at least widely used.)
   */
  class Tracer {
  public:
    /**
     * Create a new Tracer.
     *
     * @param name    The name of the tracer to create.  We do not hold on to a
     *                  reference to this string.
     * @param conf    The configuration to use for the new tracer.  We do not
     *                  hold on to a reference to this configuration.
     */
    Tracer(const std::string &name, const Conf &conf)
      : tracer_(htracer_create(name.c_str(), conf.conf_))
    {
      if (!tracer_) {
        throw std::bad_alloc();
      }
    }

    std::string Name() {
      return std::string(htracer_tname(tracer_));
    }

    /**
     * Free the Tracer.
     *
     * This destructor must not be called until all the other objects which hold
     * a reference (such as samplers and trace scopes) are freed.  It is often
     * not necessary to destroy this object at all unless you are writing a
     * library and want to support unloading your library, or you are writing an
     * application and want to support some kind of graceful shutdown.
     *
     * We could make this friendlier with some kind of reference counting via
     * atomic variables, but only at the cost of reduced performance.
     */
    ~Tracer() {
      htracer_free(tracer_);
      tracer_ = NULL;
    }

  private:
    friend class Sampler;
    friend class Scope;
    Tracer(const Tracer &other); // Can't copy
    const Tracer &operator=(const Tracer &other);
    struct htracer *tracer_;
  };

  /**
   * An HTrace sampler.
   *
   * Samplers determine when new spans are created.
   * See htrace.h for more information.
   *
   * Samplers are thread-safe.  They can be used by multiple threads
   * simultaneously.
   */
  class Sampler {
  public:
    /**
     * Create a new Sampler.
     *
     * @param tracer  The tracer to use.  You must not free this tracer until
     *                  after this sampler is freed.
     * @param conf    The configuration to use for the new sampler.  We do not
     *                  hold on to a reference to this configuration.
     */
    Sampler(Tracer *tracer, const Conf &conf)
        : smp_(htrace_sampler_create(tracer->tracer_, conf.conf_)) {
      if (!smp_) {
        throw std::bad_alloc();
      }
    }

    /**
     * Get a description of this Sampler.
     */
    std::string ToString() {
      return std::string(htrace_sampler_to_str(smp_));
    }

    ~Sampler() {
      htrace_sampler_free(smp_);
      smp_ = NULL;
    }

  private:
    friend class Tracer;
    friend class Scope;
    Sampler(const Sampler &other); // Can't copy
    const Sampler &operator=(const Sampler &other);

    struct htrace_sampler *smp_;
  };

  class Scope {
  public:
    Scope(Tracer &tracer, const char *name)
      : scope_(htrace_start_span(tracer.tracer_, NULL, name)) {
    }

    Scope(Tracer &tracer, const std::string &name)
      : scope_(htrace_start_span(tracer.tracer_, NULL, name.c_str())) {
    }

    Scope(Tracer &tracer, Sampler &smp, const char *name)
      : scope_(htrace_start_span(tracer.tracer_, smp.smp_, name)) {
    }

    Scope(Tracer &tracer, Sampler &smp, const std::string &name)
      : scope_(htrace_start_span(tracer.tracer_, smp.smp_, name.c_str())) {
    }

    ~Scope() {
      htrace_scope_close(scope_);
      scope_ = NULL;
    }

    uint64_t GetSpanId() {
      return htrace_scope_get_span_id(scope_);
    }

  private:
    friend class Tracer;
    Scope(htrace::Scope &other); // Can't copy
    Scope& operator=(Scope &scope); // Can't assign

    struct htrace_scope *scope_;
  };
}

#endif

// vim: ts=2:sw=2:et
