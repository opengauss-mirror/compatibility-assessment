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
mvn clean package -f ../agent/pom.xml
mvn clean package -f ../attach/pom.xml
mvn clean package
mkdir -p transcribe-replay-tool/plugin
cp target/transcribe-replay-tool-jar-with-dependencies.jar transcribe-replay-tool/transcribe-replay-tool-${version}.jar
cp -r config transcribe-replay-tool/
cp -r script transcribe-replay-tool/
cp -r ../agent/target/agent.jar transcribe-replay-tool/plugin/
cp -r ../attach/target/attach.jar transcribe-replay-tool/plugin/
chmod u+x transcribe-replay-tool/plugin/attach.jar
chmod u+x transcribe-replay-tool/plugin/agent.jar
tar -czf openGauss-TranscribeReplay-${version}.tar.gz transcribe-replay-tool
rm -rf transcribe-replay-tool
mv openGauss-TranscribeReplay-${version}.tar.gz target/