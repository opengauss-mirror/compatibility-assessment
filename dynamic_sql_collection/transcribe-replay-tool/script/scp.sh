#!/bin/bash

# 初始化变量
source_path=""
target_path=""
file_name=""
username=""
ip=""
readonly JAR_NAME="transcribe-replay-tool-7.0.0-RC2.jar"

escape_shell_arg() {
    local arg="$1"
    echo "$arg" | sed 's/\\/\\\\/g' | sed "s/'/'\\''/g"
}

is_valid_ipv4() {
    [[ $1 =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]]
}

is_valid_ipv6() {
    [[ $1 =~ ^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$ || $1 =~ ^([0-9a-fA-F]{1,4}:)+(::)([0-9a-fA-F]{1,4}:)*[0-9a-fA-F]{1,4}$ ]]
}

is_valid_hostname() {
    [[ $1 =~ ^[a-zA-Z0-9.-]+$ ]]
}

clean_path() {
    local path="$1"
    echo "$path" | sed 's/\.\.\///g' | sed 's/\/\.\.\///g' | sed 's/\/\.\.$//g' | sed 's/^\.\.\///g'
}

function transfer_and_delete() {
    local index=$1
    local target=$((index-1))
    local previous_name
    if [ $target -eq 0 ]; then
        previous_name="${file_name}"
    else
        previous_name="${file_name}${target}"
    fi
    if ! scp "${username}@${ip}:${source_path}/${previous_name}" "${target_path}/"; then
        echo "Warning: Failed to copy file ${previous_name}, continuing..." >&2
    fi
    if ! ssh "${username}@${ip}" "rm -rf '$(escape_shell_arg "${source_path}/${previous_name}")'"; then
        echo "Warning: Failed to delete remote file ${previous_name}, continuing..." >&2
    fi
}

function main() {
    local index=0
    while true; do
      ((index++))
      local target_name="${file_name}${index}"
      if ssh "${username}@${ip}" "[ -f '$(escape_shell_arg "${source_path}/${target_name}")' ]"; then
          transfer_and_delete $index
      else
          local OUTPUT
          OUTPUT=$(ssh "${username}@${ip}" "ps -ef | grep '$(escape_shell_arg "${JAR_NAME}")' | grep -v grep" 2>/dev/null) || true
          if [ -z "$OUTPUT" ]; then
              transfer_and_delete $index
              break
          else
              ((index--))
              sleep 1
          fi
      fi
    done
    echo "Have obtained all tcpdump files."
}

# 安全地获取命令行参数
while getopts "s:t:n:u:h:" opt; do
      case $opt in
        s) source_path=$OPTARG
          ;;
        t) target_path=$OPTARG
          ;;
        n) file_name=$OPTARG
          ;;
        u) username=$OPTARG
           ;;
        h) ip=$OPTARG
           ;;
        \?) echo "Invalid option -$OPTARG" >&2
           exit 1
           ;;
        :) echo "Option -$OPTARG requires an argument." >&2
           exit 1
           ;;
      esac
    done

    # 保存剩余参数
    local remaining_args=("$@")
    shift $((OPTIND - 1))
    file_name="${file_name}.pcap"

if [[ -z "$source_path" || -z "$target_path" || -z "$file_name" || -z "$username" || -z "$ip" ]]; then
    echo "Error: Missing required parameters. Usage: $0 -s source_path -t target_path -n file_name -u username -h ip" >&2
    exit 1
fi

if ! is_valid_ipv4 "$ip" && ! is_valid_ipv6 "$ip" && ! is_valid_hostname "$ip"; then
    echo "Error: Invalid IP address or hostname format." >&2
    exit 1
fi

source_path="$(clean_path "$source_path")"
target_path="$(clean_path "$target_path")"
file_name="$(clean_path "$file_name")"

trap "echo 'Error occurred, script aborted.' >&2; exit 1" ERR

main "${remaining_args[@]}"
