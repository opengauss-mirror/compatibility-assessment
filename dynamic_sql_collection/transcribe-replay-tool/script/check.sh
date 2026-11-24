#!/bin/bash

# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# transcribe-replay-tool is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

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