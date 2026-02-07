


## Prompt

```
Let's create a log flow for any data intensive, high performant development

Any app -> log sink that record binary -> output to parquet, not blocking the hot thread


Features to support:
- parquet compression
- high thoughput
- support all parquet column types
- this app will be used to capture stock market by order live feed, so you must be high performant
- we will use this in modern C++ project, so you need to export to C interface
- we choose zig because it integrates great with C, compiles faster

First, create a hooks/load_zig.sh that download the master zig such as https://ziglang.org/builds/zig-aarch64-linux-0.16.0-dev.2490+fce7878a9.tar.xz and extract to ./local_data/zig/ and update PATH

Then, find out if there is any parquet implementation in zig, if so clone and compile it, if not, git clone https://github.com/apache/arrow-rs as reference impl, and write your own parquet impl in zig


Spin up an agent team to do the task

```
