# Presto [![release](https://img.shields.io/badge/release-sc--0.157-blue.svg)](https://travis-ci.org/prestodb/presto) [![license](https://img.shields.io/hexpm/l/plug.svg)](https://github.com/gh351135612/presto/blob/sc-0.157/LICENSE)
* [简介](#简介)
* [特点](#特点)
* [环境](#环境)
* [编译](#编译)
* [部署](#部署)
* [调试](#调试)

<h2 id="简介">简介</h2>

Presto 是一个分布式的SQL 查询引擎，它支持跨数据源的查询操作，但是因为版权以及适用性，不会满足大家所有的需求，如不支持的Oracle 的接入、自动类型转换等一些附加功能。该分支会根据实际情况，对官方版本进行二次开发，读者可以根据实现思路开发出满足自己需求的版本。

<h2 id="特点">特点</h2>

* 接入Oracle 数据源（支持查询，创建，插入等操作）

<h2 id="环境">环境</h2>

* Mac OS X or Linux（不支持Windows 下编译）
* Java 8 Update 92 or higher (8u92+), 64-bit
* Maven 3.3.9+ (for building)
* Python 2.4+ (for running with the launcher script)

<h2 id="编译">编译</h2>

不论是用来部署还是用来开发，都必须进行编译，因为有些代码是通过Maven 插件生成的。因为Oracle 的JDBC 驱动在公共库中已经无法查找，需要用户自行安装。
* 下载Oracle 的JDBC驱动
我插件中使用的版本为11.2.0.4.0-atlassian-hosted，用户可以自行从网上下载，也可以从我的CSDN中[下载](http://download.csdn.net/detail/u010215256/9831385)。
* 安装Oracle JDBC 驱动到本地Maven
```
mvn install:install-file -DgroupId=com.oracle  -DartifactId=ojdbc6 -Dversion=x.x -Dpackaging=jar -Dfile=ojdbc6-11.2.0.4.0-atlassian-hosted.jar
```
* 在Presto 根目录下执行Maven 命令
```
mvn clean install -DskipTests=true
```
我这里编译时跳过了测试，因为测试会花费很多时间。如果你的网络访问国外的网络比较慢可以考虑配置国内的Maven 源仓。

<h2 id='部署'>部署</h2>

Presto 提供了两种部署文件，一种是tar 包，一种是rpm，两者任选其一。tar 部署教程可参考此[教程](http://www.jianshu.com/p/46dcd1a7f47f)，rpm 部署可以参考官方[说明](https://github.com/gh351135612/presto/blob/sc-0.157/presto-server-rpm/README.md)。
* 添加Oracle 的catalog
```
connector.name=oracle
connection-url=jdbc:oracle:thin:@192.168.236.1:1521/orcl
connection-user=hadoop
connection-password=hadoop
```

<h2 id='调试'>调试</h2>

如需观察Presto 是如何运行的，可以进行断点调试。这里推荐使用[IntelliJIDEA](https://www.jetbrains.com/idea/) 导入项目，我们在run configuration 中设置如下参数就可以了,PrestoServer 是Presto 后台服务的启动入口。

#### 设置IntelliJIDEA的run configuration

* Main Class: com.facebook.presto.server.PrestoServer
* VM Options: -ea -XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent -Xmx2G -Dconfig=etc/config.properties -Dlog.levels-file=etc/log.properties
* Working directory: $MODULE_DIR$
* Use classpath of module: presto-main

#### 修改oracle.properties

调试环境下，catalog 的配置文件夹在presto/presto-main/etc/catalog 路径中，修改成目标Oracle 的配置信息。







