HTrace is a lightweight tracing library written in java.

-------------------------------
Testing Information

The test that creates a sample trace (TestHTrace) takes a command line argument telling it where to write span information.
Run mvn test -DspanFile="FILE_PATH" to write span information to
FILE_PATH. If no file is specified, span information will be written
to standard out. If span information is written to a file, you can use
the included graphDrawer python script in tools/ to create a simple
visualization of the trace.
