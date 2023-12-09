#!/bin/bash

# Usage: ./tail_logs.sh <log_file> [start_time]
# - <log_file>: Path to the log file.
# - [start_time]: Optional parameter to specify the start time. If provided, the script will tail the log file from that time.
#
# Examples:
# - ./tail_logs.sh your_log_file.log
#   Tail the entire log file from the newest entries.
# - ./tail_logs.sh your_log_file.log "2023-12-04 12:00:00"
#   Tail the log file from the specified start time.

# Check if the correct number of arguments is provided
if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    echo "Usage: $0 <log_file> [start_time]"
    exit 1
fi

log_file="$1"

if [ "$#" -eq 2 ]; then
    start_time="$2"
    start_line=$(awk -v start_time="$start_time" '$1" "$2 >= start_time {print NR; exit}' "$log_file")

    if [ -n "$start_line" ]; then
        tail -n +"$start_line" -F "$log_file"
        exit 0
    else
        echo "Specified time not found in the log file. Showing the newest logs."
        tail -n 10 -F "$log_file"
        exit 0
    fi
fi

# If start time is not provided or not found, tail the entire log file from the newest entries
tail -n 0 -F "$log_file"
