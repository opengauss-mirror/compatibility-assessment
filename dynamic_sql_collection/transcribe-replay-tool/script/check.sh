#!/bin/bash
version=7.0.0
while true; do
    # 检查transcribe-replay-tool-${version}.jar进程是否存在
    if pgrep -f "transcribe-replay-tool-${version}.jar" > /dev/null; then
        echo "transcribe-replay-tool-${version}.jar process is running."
        if pgrep tcpdump > /dev/null; then
            echo "tcpdump process is running."
            sleep 5
        else
            echo "transcribe-replay-tool-${version}.jar is running but tcpdump process is not running. Killing it."
            pkill -f "transcribe-replay-tool-${version}.jar"
            exit 0
        fi
    else
        # 检查tcpdump进程是否存在
        if pgrep tcpdump > /dev/null; then
            echo "transcribe-replay-tool-${version}.jar is not running but tcpdump process is running. Killing it."
            pkill tcpdump
            echo "tcpdump process killed. Exiting script."
            exit 0
        else
            echo "Neither transcribe-replay-tool-${version}.jar nor tcpdump is running. Exiting script."
            exit 0
        fi
    fi
done