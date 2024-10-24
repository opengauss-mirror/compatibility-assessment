#!/bin/bash
mvn clean package -f ../agent/pom.xml
mvn clean package -f ../attach/pom.xml
mvn clean package
mkdir transcribe-replay-tool
cp target/transcribe-replay-tool-jar-with-dependencies.jar transcribe-replay-tool/transcribe-replay-tool-7.0.0-RC1.jar
cp -r config transcribe-replay-tool/
cp -r script transcribe-replay-tool/
cp -r plugin transcribe-replay-tool/
cp -r ../agent/target/agent.jar transcribe-replay-tool/plugin/
cp -r ../attach/target/attach.jar transcribe-replay-tool/plugin/
chmod u+x transcribe-replay-tool/plugin/aarch64/tcpdump
chmod u+x transcribe-replay-tool/plugin/x86_64/tcpdump
chmod u+x transcribe-replay-tool/plugin/attach.jar
chmod u+x transcribe-replay-tool/plugin/agent.jar
tar -czf transcribe-replay-tool-7.0.0-RC1.tar.gz transcribe-replay-tool
rm -rf transcribe-replay-tool
mv transcribe-replay-tool-7.0.0-RC1.tar.gz target/