The HTrace C client
===============================================================================
The HTrace C client is useful for distributed systems in C and C++ that need
tracing.

To use the HTrace C client, you must link against the libhtrace.so library.
On a UNIX-based platform, you would link against the version of the library
containing only the major version number.  For example, you might link against
libhtrace.so.3, which would then link against the appropriate minor version of
the library, such as libhtrace.so.3.2.0.  The libhtrace API will not change in
backwards-incompatible ways within a major version.

Some APIs in the library take htrace_conf objects.  You can create these
objects via htrace_conf_from_str.  A string of the form "KEY1=VAL1;KEY2=VAL2"
will create a configuration object with KEY1 set to VAL1, KEY2 set to VAL2,
etc.  htrace.h defines the configuration keys you can set, such as
HTRACE_LOG_PATH_KEY.

In general, you will want to create a single global htrace_ctx object,
representing an htrace context object, for your program.  The all threads can
use this htrace_ctx object.  The htrace_ctx contains all the per-process
htrace state, such as the process name, the thread-local data, and the htrace
receiver object that the process is using.

If your process supports orderly shutdown, you can call htrace_ctx_free to
accomplish this.  However, you should be sure that there are no references to
the htrace context before freeing it.  Most daemons do not support orderly
shutdown, so this is not usually a problem.  It is likely to come up in the
context of a library which uses the native htrace client.

For a quick reference to the basics of trace spans and scopes, see htrace.h.
