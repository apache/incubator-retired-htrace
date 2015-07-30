#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Build the C client for the HTrace distributed tracing system.
#

cmake_minimum_required(VERSION 2.8 FATAL_ERROR)
set(CMAKE_BUILD_TYPE, Release)  # Default to release builds

# Define "make check" as an alias for "make test."
add_custom_target(check COMMAND ctest)
enable_testing()

if (WIN32)
    MESSAGE(FATAL_ERROR "Windows support is not yet available.")
else() # UNIX
    set(CMAKE_C_FLAGS "-g ${CMAKE_C_FLAGS} -Wall -O2 -fno-strict-aliasing")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_REENTRANT") # Enable pthreads.
    if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
        # _GNU_SOURCE is needed to see all the glibc definitions.
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_GNU_SOURCE")
        # Use the 64-bit forms of off_t, etc.
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64")
    endif()
endif()

INCLUDE(CheckCSourceCompiles)
CHECK_C_SOURCE_COMPILES("int main(void) { static __thread int i = 0; return 0; }" HAVE_IMPROVED_TLS)
CONFIGURE_FILE(${CMAKE_SOURCE_DIR}/util/build.h.cmake ${CMAKE_BINARY_DIR}/util/build.h)

get_filename_component(HTRACE_ABSPATH "../../htrace-htraced/go/build/htrace" ABSOLUTE)
get_filename_component(HTRACED_ABSPATH "../../htrace-htraced/go/build/htraced" ABSOLUTE)
CONFIGURE_FILE(${CMAKE_SOURCE_DIR}/test/test_config.h.cmake ${CMAKE_BINARY_DIR}/test/test_config.h)

find_package(PkgConfig)
pkg_check_modules(PC_JSON-C QUIET json-c)
find_path(JSON_C_INCLUDE_DIR "json.h"
    HINTS ${JSON_C_PREFIX} ${PC_JSON-C_INCLUDEDIR} ${PC_JSON-C_INCLUDE_DIRS} PATH_SUFFIXES json-c json)
find_library(JSON_C_LIBRARY NAMES json-c json libjson
    HINTS ${JSON_C_PREFIX}${PC_JSON-C_LIBDIR} ${PC_JSON-C_LIBRARY_DIRS})
IF(JSON_C_INCLUDE_DIR AND JSON_C_LIBRARY)
ELSE(JSON_C_INCLUDE_DIR AND JSON_C_LIBRARY)
    MESSAGE(FATAL_ERROR "Failed to find libjson-c. Try installing libjson-c with apt-get or yum, or install it manually from http://oss.metaparadigm.com/json-c/")
ENDIF(JSON_C_INCLUDE_DIR AND JSON_C_LIBRARY)

include_directories(${CMAKE_BINARY_DIR}
                    ${CMAKE_SOURCE_DIR}
                    ${JSON_C_INCLUDE_DIR})

if (${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
    set(RAND_SRC "util/rand_linux.c")
else()
    set(RAND_SRC "util/rand_posix.c")
endif()

set(SRC_ALL
    ${RAND_SRC}
    core/conf.c
    core/htracer.c
    core/scope.c
    core/span.c
    core/span_id.c
    receiver/hrpc.c
    receiver/htraced.c
    receiver/local_file.c
    receiver/noop.c
    receiver/receiver.c
    sampler/always.c
    sampler/never.c
    sampler/prob.c
    sampler/sampler.c
    util/cmp.c
    util/cmp_util.c
    util/htable.c
    util/log.c
    util/tracer_id.c
    util/string.c
    util/terror.c
    util/time.c
)

set(DEPS_ALL pthread)
IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
  set(DEPS_ALL ${DEPS_ALL} rt)
ENDIF()

# The unit test version of the library, which exposes all symbols.
add_library(htrace_test STATIC
    ${SRC_ALL}
    test/mini_htraced.c
    test/span_table.c
    test/span_util.c
    test/temp_dir.c
    test/test.c
)
target_link_libraries(htrace_test ${DEPS_ALL} ${JSON_C_LIBRARY})

# Hide all symbols by default.  Only the symbols we specifically mark as
# visible should be accessable by the library user.  This should avoid
# conflicts between our function and global variable names and those of
# the host program.
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fvisibility=hidden")

# The production version of the library, which exposes only the public API.
add_library(htrace SHARED ${SRC_ALL})
target_link_libraries(htrace ${DEPS_ALL})
set(HTRACE_VERSION_MAJOR "3")
set(HTRACE_VERSION_MINOR "2")
set(HTRACE_VERSION_PATCH "0")
set(HTRACE_VERSION_STRING
    "${HTRACE_VERSION_MAJOR}.${HTRACE_VERSION_MINOR}.${HTRACE_VERSION_PATCH}")
set_target_properties(htrace PROPERTIES
    VERSION ${HTRACE_VERSION_STRING}
    SOVERSION ${HTRACE_VERSION_MAJOR})

macro(add_utest utest)
    add_executable(${utest}
        ${ARGN}
    )
    target_link_libraries(${utest} htrace_test)
    add_test(${utest} ${CMAKE_CURRENT_BINARY_DIR}/${utest} ${utest})
endmacro(add_utest)

add_utest(cmp_util-unit
    test/cmp_util-unit.c
)

add_utest(conf-unit
    test/conf-unit.c
)

add_utest(htable-unit
    test/htable-unit.c
)

add_utest(htraced_rcv-unit
    test/htraced_rcv-unit.c
    test/rtest.c
)

add_executable(linkage-unit test/linkage-unit.c)
target_link_libraries(linkage-unit htrace dl)
add_test(linkage-unit ${CMAKE_CURRENT_BINARY_DIR}/linkage-unit linkage-unit)

add_utest(local_file_rcv-unit
    test/local_file_rcv-unit.c
    test/rtest.c
)

add_utest(local_file_rcvpp-unit
    test/local_file_rcv-unit.c
    test/rtestpp.cc
)

add_utest(log-unit
    test/log-unit.c
)

add_utest(mini_htraced-unit
    test/mini_htraced-unit.c
)

add_utest(tracer_id-unit
    test/tracer_id-unit.c
)

if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
    add_utest(rand_linux-unit
        test/rand-unit.c
        util/rand_linux.c
    )
endif()
add_utest(rand_posix-unit
    test/rand-unit.c
    util/rand_posix.c
)

add_utest(sampler-unit
    test/sampler-unit.c
)

add_utest(span-unit
    test/span-unit.c
)

add_utest(span_id-unit
    test/span_id-unit.c
)

add_utest(string-unit
    test/string-unit.c
)

add_utest(temp_dir-unit
    test/temp_dir-unit.c
)

add_utest(time-unit
    test/time-unit.c
)

# Install libhtrace.so and htrace.h.
# These are the only build products that external users can consume.
install(TARGETS htrace DESTINATION lib)
install(FILES ${CMAKE_SOURCE_DIR}/core/htrace.h DESTINATION include)
