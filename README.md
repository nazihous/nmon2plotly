# Nmon to Plotly (nmon2plotly.py)

## Purpose

`nmon2plotly.py` is a Python script designed to parse performance data from `.nmon` files and generate interactive HTML reports using the Plotly.js library. Nmon (Nigel's Monitor) is a widely used tool for monitoring performance on AIX and Linux systems. This script provides a way to visualize these performance metrics effectively.

## Main Functionalities

The script performs the following key functions:

1.  **Parses `.nmon` Files**: It reads and processes data from one or more `.nmon` files. Nmon files contain a rich set of system performance metrics, captured over time.
2.  **Generates NDJSON Output**: The parsed data is transformed into Newline Delimited JSON (NDJSON) format. This format is convenient for data streaming and bulk loading into databases. The script creates separate NDJSON files for general system metrics and for detailed process (TOP) statistics.
3.  **Creates Interactive HTML Reports**: The primary output is a single, comprehensive HTML file. This report embeds interactive charts generated with Plotly.js, allowing users to explore and analyze the performance data visually.

## Performance Metrics Processed

The script is capable of parsing and visualizing a wide array of performance metrics, including (but not limited to):

*   **CPU Utilization**: Overall CPU usage (User%, Sys%, Wait%, Idle%) and detailed usage per logical CPU core.
*   **Memory Usage**: Real and virtual memory statistics, including percentages (Process%, FScache%, System%, Free%) and absolute values in MB (Total, Used, Free).
*   **LPAR (Logical Partition) Statistics**: For AIX systems, it processes LPAR-specific metrics like Physical CPU, Virtual CPUs, and Entitlement.
*   **Process Metrics**: Information about running processes, including run queue length, system call rates, process switch rates, fork/exec rates, and detailed CPU and memory usage for top processes (similar to the `top` command).
*   **Disk I/O**: Metrics for disk performance, such as read/write rates (KB/s), disk busy percentages, and disk wait times. It also handles Volume Group (VG) statistics.
*   **Network I/O**: Data for network interfaces, including read/write rates (KB/s), packet counts per second, and network transfer sizes.
*   **File System Usage**: Percentage of space used for JFS (Journaled File System).
*   **Paging Activity**: System-wide paging metrics (pgin, pgout, pgsin, pgsout).
*   **Shared Ethernet Adapter (SEA) Metrics**: Statistics for SEAs on AIX systems, including read/write rates and packet counts.
*   **Fibre Channel (FC) Adapter Metrics**: Read/write rates and transfer counts for FC adapters.

## Usage

The `nmon2plotly.py` script is run from the command line.

```bash
python nmon2plotly.py --input_dir /path/to/nmon_files --output_dir /path/to/output_directory [--processes N]
```

### Arguments

*   `--input_dir <directory>`: **Required**. Specifies the directory containing the `.nmon` performance data files. The script will attempt to process all files with the `.nmon` extension in this directory.
*   `--output_dir <directory>`: **Required**. Specifies the directory where the generated NDJSON files and the `index.html` report will be saved. The script will create `all` and `top` subdirectories within this output directory for the NDJSON files.
*   `--processes <N>`: **Optional**. Specifies the number of parallel processes to use for parsing multiple `.nmon` files. If not provided, it defaults to the number of CPU cores available on the system. This can significantly speed up processing when dealing with a large number of `.nmon` files.

### Sample Input File

The repository includes a sample Nmon data file: `nmon_BBAPOR046_250513.nmon`. You can use this file to test the script:

```bash
python nmon2plotly.py --input_dir . --output_dir ./output_reports
```
(This command assumes `nmon_BBAPOR046_250513.nmon` is in the current directory and will create an `output_reports` directory for the results.)

## The Interactive HTML Report

The primary output of `nmon2plotly.py` is an `index.html` file. This file contains a dashboard with multiple interactive charts, allowing for in-depth analysis of the Nmon data.

### Key Features

*   **Interactive Charts**: Powered by Plotly.js, all charts are interactive. You can:
    *   **Hover**: Mouse over data points to see specific values and timestamps.
    *   **Zoom**: Click and drag to zoom into specific time ranges on any chart.
    *   **Pan**: After zooming, you can pan across the time axis.
    *   **Linked Axes**: Zooming or panning on one chart will automatically update the x-axis (time range) of all other charts on the page, keeping them synchronized.
    *   **Download Plot**: Individual charts can be downloaded as PNG images using the camera icon that appears on hover.
    *   **Fullscreen**: Double-click on any chart to view it in fullscreen mode. Double-click again to exit.
*   **Filtering**:
    *   **LPAR Selection**: If multiple LPARs (Logical Partitions) are present in the processed `.nmon` files, you can select a specific LPAR to view its data.
    *   **Frame Selection**: Data can be filtered by the serial number of the physical frame/server.
    *   **Date Range**: You can select a start and end date to narrow down the displayed time period.
*   **Customizable Layout**:
    *   **Charts per Row**: Choose how many charts you want to see side-by-side using the "Charts per Row" dropdown. This helps in tailoring the view for different screen sizes.
*   **Dark Mode**: A toggle switch allows you to switch the report to a dark theme for better viewing in low-light conditions.
*   **Comparison Mode**: A toggle switch (labeled A/B) enables a comparison mode. This allows you to select two different NMON data sources (e.g., different LPARs or different time ranges from the same LPAR, or even data from two different servers if their `.nmon` files were processed together) and display their charts side-by-side for direct comparison. A second set of filter controls appears for the 'B' dataset.

### Available Charts

The HTML report includes a comprehensive set of charts, visualizing various aspects of system performance. Some of the key charts include:

*   **CPU Usage**: Stacked chart showing User%, Sys%, Wait%, and Idle% CPU utilization.
*   **Average Use of Logical CPU Core Threads**: Stacked bar chart showing average User% and System% per logical CPU.
*   **LPAR Usage**: Physical CPU, Virtual CPUs, and Entitlement over time.
*   **Pool CPUs & Pool Idle**: Shows shared processor pool statistics.
*   **Run Queue**: Number of runnable processes.
*   **Syscall / Read / Write**: Rates of system calls, read, and write operations.
*   **Process Switches**: Number of process switches per second.
*   **fork() & exec()**: Rates of process creation.
*   **InterProcess Comms**: Semaphores/s & Message Queues send/s.
*   **File I/O (readch & writech)**: Character read/write rates for file I/O.
*   **TOP Commands by %CPU**: Line chart showing CPU usage by different commands (processes).
*   **TOP Commands by %CPU (Stacked)**: Stacked area chart for CPU usage by command.
*   **Top 20 Process PIDs by CPU Correlation**: Bubble chart showing CPU, Character I/O, and Max Memory for top PIDs.
*   **Top 20 Process PIDs by CPU (Unstacked/Stacked)**: Line charts for CPU usage of the top 20 PIDs.
*   **FS Cache Memory Use (numperm)**: Filesystem cache memory usage percentages (numperm, minperm, maxperm).
*   **Memory Usage (MEMNEW)**: Stacked chart of memory components (Process%, FScache%, System%, Free%).
*   **Memory Usage (MB) (MEM)**: Real and Virtual memory usage in MB (Total, Used).
*   **Memory Used% (MEM)**: Real and Virtual memory used percentages.
*   **Swap-in**: Swap-in activity.
*   **All Paging per second**: pgin, pgout, pgsin, pgsout metrics.
*   **Network Read/Write (KB/s)**: Network throughput for each interface (stacked and unstacked versions).
*   **Network Packets Read/Writes/s**: Packet rates for each interface.
*   **Network Size Read/Writesize**: Average network packet sizes.
*   **Fibre Channel Read/Write (KB/s)**: Throughput for Fibre Channel adapters (stacked, unstacked, and summary versions).
*   **Fibre Channel Xfers In/Out**: Transfer rates for Fibre Channel adapters.
*   **DISK Read/Write (KB/s)**: Disk throughput for each disk (stacked and unstacked versions).
*   **DISK Busy (%)**: Percentage of time disks are busy.
*   **DISK Wait (msec/xfer)**: Disk wait times.
*   **VG Read/Write (KB/s)**: Volume group throughput (stacked and unstacked versions).
*   **VG Busy (%)**: Volume group busy percentages.
*   **JFS Percent Full**: Filesystem usage for JFS.
*   **SEA (READ/WRITE (KB/s))**: Shared Ethernet Adapter throughput (stacked, unstacked, and summary versions).
*   **SEA Packets/s**: Packet rates for SEAs.
*   **SEAPHY (READ/WRITE KB/s)**: Physical SEA adapter throughput (and summary version).
*   **SEA PHY Errors (Transmit/Receive)**: Error counts on physical SEA adapters.
*   **SEA PHY Packets Dropped**: Dropped packet counts on physical SEA adapters.

(The exact list of charts can evolve, and this list provides a general overview of the types of data visualized.)

## Dependencies

`nmon2plotly.py` is written in Python 3 and relies on the following:

*   **Python Standard Libraries**:
    *   `os`: For operating system interactions (file paths, directory creation).
    *   `json`: For working with JSON data (primarily for NDJSON output).
    *   `glob`: For finding `.nmon` files using wildcard patterns.
    *   `re`: For regular expression operations used in parsing.
    *   `multiprocessing`: For parallel processing of multiple `.nmon` files to improve performance.
    *   `argparse`: For parsing command-line arguments.
*   **Plotly.js**: The script generates HTML that uses the Plotly.js JavaScript library for rendering charts. The HTML report directly includes Plotly.js from a CDN (`https://cdn.plot.ly/plotly-latest.min.js`). Therefore, an active internet connection is required when viewing the HTML report to load the Plotly library, unless you have a local copy or a caching mechanism. No separate Python `plotly` package installation is strictly required to *run* `nmon2plotly.py` and generate the report, as the report itself fetches the JS library.

No other external Python packages are required for the execution of this script as provided.

## About .nmon Files

Nmon (Nigel's Monitor) is a performance monitoring tool for AIX and Linux operating systems. It captures a wide range of system performance data.

*   **Data Format**: `.nmon` files are typically plain text, comma-separated value (CSV) like files.
*   **Content**: They contain snapshots of system metrics taken at regular intervals. Each line usually starts with a key indicating the type of metric (e.g., `CPU_ALL` for overall CPU stats, `MEM` for memory, `NET` for network, `TOP` for top process stats, etc.), followed by a timestamp or a tag, and then the metric values.
*   **Sections**: The file includes header sections (often starting with `AAA`) that provide metadata about the system, such as hostname, date, Nmon version, CPU information, etc. Data sections for different metrics follow, with `ZZZZ` lines indicating the specific timestamp for subsequent data rows.

`nmon2plotly.py` is designed to parse this structure, correlate data across different metric types using timestamps, and present it in a human-readable, visual format.
