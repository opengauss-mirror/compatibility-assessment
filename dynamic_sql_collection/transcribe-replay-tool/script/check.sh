#!/bin/bash

while true; do
    # 检查transcribe-replay-tool-7.0.0-RC2.jar进程是否存在
    if pgrep -f 'transcribe-replay-tool-7.0.0-RC2.jar' > /dev/null; then
        echo "transcribe-replay-tool-7.0.0-RC2.jar process is running."
        if pgrep tcpdump > /dev/null; then
            echo "tcpdump process is running."
            sleep 5
        else
            echo "transcribe-replay-tool-7.0.0-RC2.jar is running but tcpdump process is not running. Killing it."
            pkill -f "transcribe-replay-tool-7.0.0-RC2.jar"
            exit 0
        fi
    else
        # 检查tcpdump进程是否存在
        if pgrep tcpdump > /dev/null; then
            echo "transcribe-replay-tool-7.0.0-RC2.jar is not running but tcpdump process is running. Killing it."
            pkill tcpdump
            echo "tcpdump process killed. Exiting script."
            exit 0
        else
            echo "Neither transcribe-replay-tool-7.0.0-RC2.jar nor tcpdump is running. Exiting script."
            exit 0
        fi
    fi
done