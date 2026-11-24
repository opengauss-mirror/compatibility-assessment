#!/bin/bash

# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# compatibility-assessment is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

set -e
ROOT_DIR=$(pwd)

function make_mybatis() {
    mkdir lib temp && cd temp
    wget https://github.com/mybatis/mybatis-3/archive/refs/tags/mybatis-3.5.13.tar.gz
    mkdir mybatis mybatis-modified
    tar -xf mybatis-3.5.13.tar.gz -C mybatis --strip-component=1
    cp ${ROOT_DIR}/patch/mybatis-3.5.13.patch .
    cd ./mybatis-modified
    cp -r ../mybatis/src ../mybatis/pom.xml ./
    patch -p1 < ../mybatis-3.5.13.patch
    mvn clean package -Dmaven.test.skip=true
    mvn -U install:install-file -DgroupId=org.mybatis -DartifactId=mybatis -Dversion=3.5.13-modified -Dpackaging=jar -Dfile=./target/mybatis-sql-extract-3.5.13.jar
}

function make_package() {
    cd ${ROOT_DIR}
    mvn clean package
    rm -rf ${ROOT_DIR}/lib
    rm -rf ${ROOT_DIR}/temp
}


function main() {
    is_make_mybatis=true
    if [[ "$1X" == "-sX" || "$1X" == "--skipX" ]]; then
        is_make_mybatis=false
    fi

    if [ "${is_make_mybatis}X" == "trueX" ]; then
        make_mybatis
    fi

    make_package
}

main $@