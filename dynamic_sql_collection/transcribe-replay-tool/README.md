# SQL流量录制回放工具
## 整体介绍
SQL流量录制回放工具，可以录制源端数据库客户端的业务sql，到目标端数据库进行回放，它支持三种录制方式：
1. 基于tcpdump与数据库通信协议解析的流量录制，可以捕获源端数据库的网络流量，形成网络数据包文件，并根据源端数据库的通信
协议，从网络数据包文件中解析出业务SQL；

<img src="./image/transcribe.jpg">

图1. tcpdump录制回放示意图

2. 基于attach应用程序的录制，可以对java应用程序进行动态插桩，直接采集java应用程序中的动态sql，工具需与java应用程序部署在同一台机器上;
3. 基于MySQL系统表的录制，通过调整MySQL全局参数general_log=ON，log_output='table'，可以使MySQL的系统表mysql.general_log记录全量sql，工具可以通过查询该表来录制业务sql。

## 录制场景
1. 使用tcpdump采集网络流量时，相关的抓包场景和约束如下：

<table>
    <tr>
        <th>MySQL部署位置</th>
        <th>抓包位置</th>
        <th>约束</th>
    </tr>
    <tr>
        <td>服务器</td>
        <td>服务器</td>
        <td>1. root或sodo提权用户
        2. 客户端禁用ssl</td> 
    </tr>
    <tr>
        <td>docker</td>
        <td>服务器</td>
        <td>1. root或sodo提权用户
        2. 客户端禁用ssl</td>
    </tr>
    <tr>
        <td>docker</td>
        <td>docker</td>
        <td>1. root或sodo提权用户
        2. 客户端禁用ssl
        3. 需要有docker os shell</td>
    </tr>
</table>
2. 工具部署形态：

<img src="./image/install.png">

3. 性能：

   使用tcpdump采集流量时，对MySQL的极致性能影响在3%以内

## 回放场景

在生产环境录制一个周期后，就可以将sql拿到实验环境进行回放，回放场景如下：

<table>
    <tr>
        <th>回放方式</th>
        <th>场景描述</th>
        <th>约束</th>
        <th>优势</th>
        <th>操作流程</th>
    </tr>
    <tr>
        <td>全量回放</td>
        <td>在实验环境将录制到的所有SQL进行回放</td>
        <td>非实时回放，不能代替增量迁移，只能在实验环境上回放</td>
        <td>可完全模拟录制期间的业务流，并对查询操作做N倍压测</td>
        <td>全量迁移(业务环境MySQL到测试环境openGauss) --> 录制 --> 回放全部sql（监控openGauss，发现问题）</td>
    </tr>
    <tr>
        <td>只回放查询</td>
        <td>在实验环境或业务环境将其中的查询语句进行回放</td>
        <td>不能完全模拟录制期间的业务业务</td>
        <td>可配合增量迁移直接在业务环境上进行N倍的查询压测，在割接之前暴露问题，但查询不是实时的</td>
        <td>全量迁移(业务环境MySQL到业务环境openGauss) --> 录制 + 增量迁移 --> 回放查询sql + 增量迁移（监控openGauss发现问题） --> 割接 --> 反向迁移</td>
    </tr>
</table>

## 工具包介绍

- 安装包获取
~~~
wget https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/transcribe-replay-tool-7.0.0-RC1.tar.gz
~~~

### transcribe-replay-tool-7.0.0-RC1.jar
录制回放核心工具，用于调度各种插件进行录制，并负责解析网络数据包文件，以及sql回放。

- 编译
~~~
cd transcribe-replay-tool
sh build.sh
~~~
编译后的工具包中已带有录制插件，使用者也可自己编译插件。

### 录制插件
#### tcpdump
网络流量采集工具，用于采集源端数据库的网络流量。

- 下载
~~~
wget https://www.tcpdump.org/release/tcpdump-4.9.3.tar.gz
~~~

- 解压 & 编译
~~~
tar -zxf tcpdump-4.9.3.tar.gz
cd tcpdump-4.9.3
./configure
make
~~~

#### attach.jar & agent.jar
动态插桩工具，用于直接采集java应用程序中的动态sql。
- 源码地址
~~~
https://gitee.com/opengauss/compatibility-assessment/tree/master/dynamic_sql_collection
~~~

### 功能脚本
1. check.sh
检查tcpdump插件进程运行状态，防止录制端异常停止时tcpdump进程未退出导致的异常采集，可与录制端同步开启，也可在录制进程结束后开启去清理残余进程
2. scp.sh
用于单向免密场景下，从录制端服务器转移网络数据包文件到本地，可在录制开始后执行该脚本，需传入以下参数：
~~~
-u: 录制端用户名
-h: 录制端服务器ip
-s: 录制端网络文件所在路径
-t: 免密端网络文件落盘路径
-n: 录制的网络文件名
~~~

- 编译
~~~
# 进入dynamic_sql_collection/agent和dynamic_sql_collection/attach进行编译
mvn clean package
~~~

## 工具使用
- 启动命令
~~~
java -jar transcribe-replay-tool-7.0.0-RC1.jar -t [transcribe|parse|replay] -f [transcribe.properties|parse.properties|replay.properties]
~~~

- 参数介绍
~~~
-t: 命令类型，可选transcribe/parse/replay，transcribe表示开启流量录制，parse表示开启解析网络数据包文件，replay表示开启sql回放
-f: 配置文件路径，根据命令类型分别可选录制端、解析端与回放端的配置文件路径，当采取attach方式录制时，只支持绝对路径
~~~

### 录制端
#### 启动命令
~~~
java -jar transcribe-replay-tool-7.0.0-RC1.jar -t transcribe -f transcribe.properties
~~~

#### 录制端配置项
~~~
# 全局配置项：三种录制方式均需配置
# sql.transcribe.mode: 录制方式，可选tcpdump/attach/general，分别表示流量采集，动态插桩与查询系统表的录制方式，String类型，无默认值
sql.transcribe.mode=tcpdump

# tcpdump配置，选择tcpdump录制方式时，另需配置以下项
# tcpdump
# tcpdump.plugin.path: tcpdump录制插件目录位置， String类型，默认值: 工具jar包路径下的plugin/子目录
tcpdump.plugin.path=/***/***/***
# tcpdump.network.interface: tcpdump工具监听的业务网口名称
tcpdump.network.interface=eth0
# tcpdump.database.port: tcpdump工具监听的数据库端口，int类型，无默认值
tcpdump.database.port=3306
# tcpdump.capture.duration: 录制时长，int类型，默认值: 1，单位: 分钟
tcpdump.capture.duration=1
# tcpdump.file.path: 网络数据包文件保暂存位置，录制得到的网络文件暂存在该路径下，若配置了远程主机信息，文件会被发送到远程主机，String类型，
# 默认值: 工具jar包所在路径下的tcpdump-files/子目录
tcpdump.file.path=/***/***/***
# tcpdump.file.name: 网络文件包名，String类型，默认值: tcpdump-file
tcpdump.file.name=tcpdump-file
# 单个网络文件包大小限制，int类型，默认值: 10，单位: MB
tcpdump.file.size=10
# max.cpu.threshold: 系统CPU使用率阈值，取值在0~1之间，当系统CPU使用率超过该值时，工具会停止录制，double类型，默认值: 0.85
max.cpu.threshold=0.85
# max.memory.threshold: 系统内存使用率阈值，取值在0~1之间，当系统内存使用率超过该值时，工具会停止录制，double类型，默认值: 0.85
max.memory.threshold=0.85
# max.disk.threshold: 磁盘使用率阈值，取值在0~1之间，当存储文件的磁盘占用率超过该值时，工具会停止录制，double类型，默认值: 0.85
max.disk.threshold=0.85
# remote.file.path: 远程主机的文件路径，录制生成的结果文件最终会发送到该路径下，String类型，无默认值
remote.file.path=/***/***/***
# remote.receiver.name: 远程主机的用户名，录制生成的结果文件最终会发送给该用户，String类型，默认值: root
remote.receiver.name=remote_user
# remote.receiver.password: 远程用户密码，String类型，无默认值
remote.receiver.password=******
# remote.node.ip: 远程主机ip，使用该ip将文件发送给远程主机，String类型，默认值: 127.0.0.1
remote.node.ip=127.0.0.1
# remote.node.port: 远程主机端口，int类型，默认值: 22
remote.node.port=22
# remote.retry.count: 发送失败重试次数，项远程主机发送文件允许的发送失败次数，超过该次数则停止录制，int类型，默认值: 1
remote.retry.count=1

# attach配置，选择attach录制方式时，另需配置以下项
# attach
# attach.plugin.path: attach.jar & agent.jar录制插件的位置，String类型，默认值: 工具jar包所在目录
attach.plugin.path=/***/***/***
# attach.process.pid: attach工具监控的java应用程序的pid,long类型，无默认值
attach.process.pid=1
# attach.target.schema: attach工具监控的java应用程序连接的数据库名称，String类型，无默认值
attach.target.schema=schema_name
# attach.capture.duration: attach工具采集sql的时长，int类型，默认值: 1，单位: 分钟
attach.capture.duration=1
# sql.file.path: attach采集到的sql文件存放目录，默认在工具jar包所在路径下的sql-files/子目录
sql.file.path=/***/***/***
# sql.file.name: attach工具采集到的sql文件名，String类型，默认值: sql-file
sql.file.name=sql-file
# sql.file.size: attach工具采集到的单个sql文件大小限制，int类型，默认值: 10，单位: MB
sql.file.size=10
# max.cpu.threshold: 系统CPU使用率阈值，取值在0~1之间，当系统CPU使用率超过该值时，工具会停止录制，double类型，默认值: 0.85
max.cpu.threshold=0.85
# max.memory.threshold: 系统内存使用率阈值，取值在0~1之间，当系统内存使用率超过该值时，工具会停止录制，double类型，默认值: 0.85
max.memory.threshold=0.85
# max.disk.threshold: 磁盘使用率阈值，取值在0~1之间，当存储文件的磁盘占用率超过该值时，工具会停止录制，double类型，默认值: 0.85
max.disk.threshold=0.85
# remote.file.path: 远程主机的文件路径，录制生成的结果文件最终会发送到该路径下，String类型，无默认值
remote.file.path=/***/***/***
# remote.receiver.name: 远程主机的用户名，录制生成的结果文件最终会发送给该用户，String类型，默认值: root
remote.receiver.name=remote_user
# remote.receiver.password: 远程用户密码，String类型，无默认值
remote.receiver.password=******
# remote.node.ip: 远程主机ip，使用该ip将文件发送给远程主机，String类型，默认值: 127.0.0.1
remote.node.ip=127.0.0.1
# remote.node.port: 远程主机端口，int类型，默认值: 22
remote.node.port=22
# remote.retry.count: 发送失败重试次数，项远程主机发送文件允许的发送失败次数，超过该次数则停止录制，int类型，默认值: 1
remote.retry.count=1

# general log配置，选择general录制方式时，另需配置以下项
# general.database.ip: 目标MySQL的ip，String类型，无默认值
general.database.ip=127.0.0.1
# general.database.port: 目标MySQL端口，int类型，默认值: 3306
general.database.port=3306
# general.database.username: 目标MySQL数据库查询用户，String类型，无默认值
general.database.username=db_user
# general.database.password: 目标MySQL数据库密码，String类型，无默认值
general.database.password=******
# general.sql.batch: 每次查询的数据条数，int类型，默认值: 10
general.sql.batch=10
# general.start.time: general log采集sql的开始时间，timestamp类型，默认值: 1970-01-01 00:00:01
general.start.time=1970-01-01 00:00:01
# sql.storage.mode: sql存储方式，可选json或db，选择json表示录制的sql存在json文件中，选择db表示录制的sql存在数据库中，String类型，默认值: json
sql.storage.mode=json
# 若选择sql存储方式为json，另需配置以下项
# sql.file.path: sql文件路径，String类型，默认值: 工具jar包所在路径下的sql-files/子目录
sql.file.path=/***/***/***
# sql.file.name: sql文件名，String类型，默认值: sql-file
sql.file.name=sql-file
# sql.file.size: sql文件大小限制，int类型，默认值: 10，单位: MB
sql.file.size=10
# 若选择sql存储方式为db，另需配置以下项
# database
# sql.database.ip: sql存储库的ip，String类型，无默认值
sql.database.ip=127.0.0.1
# sql.database.port: sql存储库端口，int类型，无默认值
sql.database.port=5432
# sql.database.username: sql存储库用户名，String类型，无默认值
sql.database.username=db_user
# sql.database.name: sql存储库名称，String类型，无默认值
sql.database.name=transcribe
# sql.database.password: sql存储库密码，String类型，无默认值
sql.database.password=******
# sql.table.name: 存储sql的表名称，String类型，默认值: sql_table
sql.table.name=sql_table
# sql.table.drop: 存储sql的表名若与数据库中已有表名一致，是否删除已有的表，boolean类型，默认值: false
sql.table.drop=false
~~~

注意：若选择的录制方式为tcpdump，需开启解析端对网络数据包文件进行解析，才能获取sql

### 解析端
#### 启动命令
~~~
java -jar transcribe-replay-tool-7.0.0-RC1.jar -t parse -f parse.properties
~~~

#### 解析端配置项
~~~
# parse
# 待解析的网络数据包文件所在目录
tcpdump.file.path=/***/***/***
# tcpdump.database.type: tcpdump工具采集的源端数据库产品名称，目前仅支持mysql
tcpdump.database.type=mysql
# tcpdump.database.ip: tcpdump工具采集时监听的源端数据库的ip，String类型，无默认值
tcpdump.database.ip=127.0.0.1
# tcpdump.database.port: tcpdump工具采集时监听的源端数据库端口，int类型，无默认值
tcpdump.database.port=3306
# queue.size.limit: 解析时限定每次读取的最大报文条数，int类型，默认值: 10000
queue.size.limit=10000
# packet.batch.siz: 解析时每次提交sql所处理的报文条数，int类型，默认值: 10000
packet.batch.size=10000
# tcpdump.file.drop: 是否每解析完一个tcpdump文件就将其删除，boolean类型，默认值: false
tcpdump.file.drop=false
# parse.max.time: 解析进程的总执行时间，从进程启动开始计算，为0表示进程一直持续直到收到结束标识，int类型，单位: 分钟，默认值: 0
parse.max.time=0
# sql.storage.mode: sql存储方式，可选json或db，选择json表示录制的sql存在json文件中，选择db表示录制的sql存在数据库中，String类型，默认值: json
sql.storage.mode=json
# 若选择sql存储方式为json，另需配置以下项
# sql.file.path: sql文件路径，String类型，默认值: 工具jar包所在路径下的parse-files/子目录
sql.file.path=/***/***/***
# sql.file.name: sql文件名，String类型，默认值: parse-file
sql.file.name=parse
# sql.file.size: sql文件大小限制，int类型，默认值: 10，单位: MB
sql.file.size=10
# 若选择sql存储方式为db，另需配置以下项
# database
# sql.database.ip: sql存储库的ip，String类型，无默认值
sql.database.ip=127.0.0.1
# sql.database.port: sql存储库端口，int类型，无默认值
sql.database.port=5432
# sql.database.username: sql存储库用户名，String类型，无默认值
sql.database.username=db_user
# sql.database.name: sql存储库名称，String类型，无默认值
sql.database.name=transcribe
# sql.database.password: sql存储库密码，String类型，无默认值
sql.database.password=******
# sql.table.name: 存储sql的表名称，String类型，默认值: sql_table
sql.table.name=sql_table
# sql.table.drop: 存储sql的表名若与数据库中已有表名一致，是否删除已有的表，boolean类型，默认值: false
sql.table.drop=false
# parse.select.result: 是否解析select语句查询结果，该功能用于对比录制端和回放端的查询结果，boolean类型，默认值: false
parse.select.result=false
# select.result.path: select语句查询结果保存文件路径，String类型，无默认值
select.result.path=/***/***/***
# result.file.name: select语句查询结果保存文件名称，String类型，默认值: select-result
result.file.name=select-result
# result.file.size: select语句查询结果保存文件大小，int类型，默认值: 10，单位: MB
result.file.size=10
~~~

### 回放端
#### 启动命令
~~~
java -jar transcribe-replay-tool-7.0.0-RC1.jar -t replay -f replay.properties
~~~

#### 回放端配置项
~~~
# 回放方式db或json，String类型，默认db
sql.storage.mode=json
# 回放策略 串行-serial 并行-parallel,String类型，默认serial
sql.replay.strategy = parallel
# N倍压测倍数，int类型
sql.replay.multiple=3
# 是否只回放查询语句，boolean类型，默认false，选择N倍压测时只能设置为true
sql.replay.only.query=false
# 并行回放的最大线程数，int类型
sql.replay.parallel.max.pool.size=5
# 慢SQL判定规则, 1或2，默认值： 2
sql.replay.slow.sql.rule=2
# 慢SQL判定规则规则1(与MySQL时间差距，单位：微秒)，int类型
sql.replay.slow.time.difference.threshold=1000
# 慢SQL判定规则2:(openGauss执行耗时)， int类型
sql.replay.slow.sql.duration.threshold=1000
# 慢SQL打印TOPN，int类型
sql.replay.slow.top.number=5
# MySQL和openGauss执行时间对比图采样间隔
sql.replay.draw.interval=1000
# 回放session白名单, 格式: 192.168.0.1 or 192.168.0.1:8888, session之间用';'分隔
sql.replay.session.white.list=[]
# 回放session黑名单, 格式: 192.168.0.1 or 192.168.0.1:8888, session之间用';'分隔
sql.replay.session.black.list=[192.168.0.229:60032]
# replay.max.time: 回放进程的总执行时间，从进程启动开始计算，为0表示进程一直持续直到收到结束标识，int类型，单位: 分钟，默认值: 0
replay.max.time=0
# source.time.interval.replay:是否启用回放时间间隔和源端一致的功能，不启用则是连续快速回放模式，boolean类型，默认值: false
source.time.interval.replay=false

# 回放端数据库ip，String类型，无默认值
sql.replay.database.ip=192.168.0.34
# 回放端数据库端口，int类型，无默认值
sql.replay.database.port=5432
# 回放端数据库名称，String类型，无默认值
sql.replay.database.name=sql_replay
# 回放端数据库用户名，String类型，无默认值
sql.replay.database.username=opengauss_test
# 回放端数据库用户密码，String类型，无默认值
sql.replay.database.password=******

# 网络文件或sql文件的个数限制，文件超出该限制时，停止录制，实际产生的文件个数可能会略大于该限制，默认值：100
file.count.limit=100

# 若选择sql回放方式为json，另需配置以下项
# sql.file.path: sql文件路径，String类型，默认值: 工具jar包所在路径下的parse-files/子目录
sql.file.path=/***/***/***
# sql.file.name: sql文件名，String类型，默认值: parse-file
sql.file.name=parse-file

# 若选择sql回放方式为db，另需配置以下项
# database
# sql.database.ip: sql存储库的ip，String类型，无默认值
sql.database.ip=127.0.0.1
# sql.database.port: sql存储库端口，int类型，无默认值
sql.database.port=5432
# sql.database.username: sql存储库用户名，String类型，无默认值
sql.database.username=db_user
# sql.database.name: sql存储库名称，String类型，无默认值
sql.database.name=transcribe
# sql.database.password: sql存储库密码，String类型，无默认值
sql.database.password=******
# sql.table.name: 存储sql的表名称，String类型，默认值: sql_table
sql.table.name=sql_table
# sql.table.drop: 存储sql的表名若与数据库中已有表名一致，是否删除已有的表，boolean类型，默认值: false
sql.table.drop=false

# compare.select.result: 是否将回放端和录制端的select查询结果对比，boolean类型，默认值: false
compare.select.result=false
# select.result.path: select语句查询结果保存文件路径，String类型，无默认值
select.result.path=/***/***/***
# result.file.name: select语句查询结果保存文件名称，String类型，默认值: select-result
result.file.name=select-result
~~~

## 新特性
### 流式回放功能
录制、解析、回放三个进程可同时执行，实现边录制边解析边回放
~~~
注1：该流式处理功能不影响原有的录制、解析、回放三个进程顺序执行的结果，原有操作方式仍可使用
注2：为了防止录制端进程异常终止导致解析端和回放端进程残留的问题，工具提供了参数parse.max.time和replay.max.time配置解析端和回放端的最大工作时长，达到该阈值则进程自动终止
~~~

### 回放结果对比
将回放端的select语句查询结果与源端查询结果进行对比，并将对比结果写到data_diff.log日志文件里，该功能可通过参数控制是否开启
~~~
注1：开启对比功能则会将源端所有select语句查询结果保存到磁盘，因此开启该功能应预留足够的磁盘空间
注2：由于该功能是将源端select语句的响应包直接解析，回放端是通过调jdbc将查询结果进行了转化，二进制类型blob、longblob、mediumblob、tinyblob、raw通过jdbc的转化，与直接解析响应包得到的数据格式不相同，因此这几种类型的数据对比结果与源端不一致，这是对比功能的局限性，实际数据库里存的值是一致的，对业务无影响
~~~

### 时间间隔一致
回放端相邻sql的回放时间间隔与源端保持一致，该功能可通过参数控制是否开启
~~~
注1：源端和回放端执行sql耗时存在一定差异，为了避免相邻sql之间的数据依赖关系影响回放结果，在回放间隔基本一致的条件下，还需确保所有sql按顺序串行执行，因此两个相邻sql回放时间间隔跟源端可能存在差异，但差异是毫秒级别的，可以忽略，但在业务量较少的场景下，时间间隔可以达到一致
~~~

## 约束
### 前置操作
1. 录制前需保证源端数据库和目标端数据库基础数据一致

### tcpdump录制
1. 录制需要root用户或sudo提权用户操作
2. 在docker录制时需要docker中安装有docker os shell
3. 源数据库客户端禁用ssl
4. 目标服务器或容器需安装libpcap

### attach录制
1. 目标进程应用程序为jdk8+，环境要求为jdk11+
2. 目标进程使用的MySQL驱动为mysql-jdbc-connector 5.x及8.x
3. 录制得到的sql只支持json文件存储
4. 无法获取sql执行耗时
5. 不支持采集多线程的目标程序

### general_log录制
1. 调整MySQL全局参数general_log=ON，log_output='table'
2. 无法获取sql执行耗时

### tcpdump解析
1. 只能完整解析开启录制的时间点之后新建连接执行的sql

### 回放
1. 只支持向openGauss数据库回放
2. 只有通过tcpdump录制的sql支持并行回放
3. Insert语句是慢SQL时，不打印执行计划

### 新特性
1. 在流式回放功能开启时，会影响结果对比、时间间隔一致功能，因此流式回放、结果对比、时间间隔一致三个功能应尽量单独使用

## FAQ
1. MySQL协议解析
- MySQL协议官网介绍
~~~
https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_PROTOCOL.html
~~~

2.常见问题：
Q：在openEuler 20.03 x86_64环境上执行tcpdump录制失败报错tcpdump: error while loading shared libraries: libcrypto.so.10: cannot open shared object file: No such file or dir怎么办？
A：在transcribe-replay-tool/plugin/x86_64中有一个运行库文件libcrypto.so.1.0.2k，可以将其复制到/usr/lib64/下面
cp -r transcribe-replay-tool/plugin/x86_64/libcrypto.so.1.0.2k /usr/lib64/
再执行
cd /usr/lib64
进入lib64目录，然后创建软链接
ln -s /usr/lib64/libcrypto.so.1.0.2k /usr/lib64/libcrypto.so.10
创建成功后再执行tcpdump就可以了。

Q：为什么pbe方式执行失败的语句没有被解析？
A：pbe执行的语句，若执行失败，参数不绑定，导致无法获得完整的参数报文，无法解析，但是直接执行的普通sql不用绑定参数，可以解析

Q：为什么偶尔会有sql语句解析重复？
A：由于tcp协议报文发送机制，jdbc可能会重发sql语句导致解析时出现重复sql语句

Q：部分调用函数的查询语句会被识别为非查询语句
A：解析出的SQL语句会根据特征去识别是不是查询语句，目的在于判断该语句是否会导致数据变更，由于函数的调用有可能导致数据变更，所以会将函数的调用也识别为
非查询语句，如select version()这类，但常见的查询函数仍会被识别为查询语句，如select count(*) ... 这类，由于函数调用及查询语句的语法复杂性，会
有一定的几率误报。