#!/bin/bash

JAR_NAME="transcribe-replay-tool-6.0.0.jar"
source_path=""
target_path=""
file_name=""
username=""
ip=""

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

    shift $((OPTIND - 1))
    file_name="${file_name}.pcap"

function transfer_and_delete() {
    local index=$1
    local target=$((index-1))
    local previous_name
    if [ $target -eq 0 ]; then
        previous_name="${file_name}"
    else
        previous_name="${file_name}${target}"
    fi
    scp $username@$ip:$source_path/$previous_name $target_path/
    ssh $username@$ip "rm -rf ${source_path}/${previous_name}"
}

function main() {
    index=0
    while true; do
      ((index++))
      target_name="${file_name}${index}"
      if ssh $ip "[ -f \"$source_path/$target_name\" ]"; then
          transfer_and_delete $index
      else
          SSH_COMMAND="ssh $username@$ip 'ps -ef | grep $JAR_NAME | grep -v grep'"
          OUTPUT=$(eval $SSH_COMMAND)
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

main @?
