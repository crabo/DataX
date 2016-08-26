

# DataX

DataX 是阿里巴巴集团内被广泛使用的离线数据同步工具/平台，实现包括 MySQL、Oracle、SqlServer、Postgre、HDFS、Hive、ADS、HBase、OTS、ODPS 等各种异构数据源之间高效的数据同步功能。


#Branch变更

* 增加对HP Vertica数据库的支持/trunk/verticawriter。  使用 select ANALYZE_CONSTRAINTS(table)判断批提交是否成功。
* 增加增量读取配置，在reader的querySql中直接使用 $ts_start,$ts_end变量。 具体参见verticawriter的readme实例。
* 增加持续运行配置。设置ts_interval_sec ， taskGroup执行完后， sleep(st_interval_sec)，然后重新进行taskGroup启动（跳过job初始化）。
* 兼容保持原有全量执行模式！！


# 请转官方github： https://github.com/alibaba/DataX

