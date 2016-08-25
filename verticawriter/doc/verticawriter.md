# DataX VerticaWriter


---


## 1 快速介绍

VerticaWriter 插件实现了写入数据到 Vertica 主库的目的表的功能。在底层实现上， VerticaWriter 通过 JDBC 连接远程 Mysql 数据库，并执行相应的 insert into ...的 sql 语句将数据写入 Vertica，内部会分批次提交入库。

通过将随任务时间定期更新的 ts_start,ts_end 变量替换到querySql中，从而实现增量数据导入.
同时，可以从shell外部传入参数。初始化加载json时一次性替换shell变量到json，如下列的limit参数。
因为使用变量替换， 所以参数可以在json文件任何地方使用，只需要符合$开头变量即可。

python ../bin/datax.py -p "-Dlimit=10 -Dpwd=crm" ./test.json

SELECT * FROM kd_customer where UpdateTime>='$ts_start' and UpdateTime<'$ts_end' order by CreateTime limit $limit



## 2 实现原理

VerticaWriter 通过 DataX 框架获取 Reader 生成的协议数据，根据你配置的 `writeMode` 生成


* `insert into...`
批量执行插入后， 会执行 select ANALYZE_CONSTRAINTS('table') 检测是否存在主键冲突。
如果冲突， 转移到逐条语句select 1 where id=? 来判断， 从而执行update或insert。
如果不冲突， 一次commit整个batchSize的作业。 所以建议根据时间情况设置合适的batchSize大小



## 3 功能说明

### 3.1 配置样例


```json
{
    "job": {
        "setting": {
            "ts_interval_sec":15, #####设置则启用task无限循环执行模式，必须配合delta增量配置
            "ts_file":"verticajob.ts.txt", ####启用delta增量配置使用的持久时间戳:任务启动时间
            "speed": {
                "channel":1   ###逐个作业排队执行
            },
            "errorLimit": {
                "record": 0,
                "percentage":0.02
            }
        },
        "content": [
           {
            "taskId":0,  ####delta增量时必须从0开始设置taskId。 否则认为是全量导入sql并进行task拆分
            "reader": {
                "name": "mysqlreader",
                "parameter": {
                    "username": "jusrut7et7de",
                    "password": "*****",
                    "connection": [{  ####注意此处各项参数均为数组
                        "querySql": ["SELECT ID AS id,Nick AS name,CreateTime AS create_date FROM kd_customer where CreateTime>='$ts_start' and CreateTime<'$ts_end' order by CreateTime limit $limit"],
                        "jdbcUrl": ["jdbc:mysql://localhost:3306/test"]
                    }]
                }
            },"writer": {
                "name": "verticawriter",
                "parameter": {
        			"batchSize":10, ####insert冲突的可能性多大？推荐设置10-1000。
                    "writeMode": "insert",
                    "username": "vdbadmin",
                    "password": "$pwd",  ###此参数从shell中 -p "-Dpwd=*** -Dp2=..."传入
                    "column": ["ID",
                              "name",
                              "create_date" ],
                    "connection": [{
                        "table": ["mytable"],
                        "jdbcUrl": "jdbc:vertica://local:5433/crm"
                     }]
                }
            }
        }
        
        ,{"taskId":1,  ###继续增加其余table的配置项。
           "reader":{....}
           "writer":{.....}
         }
   }

```


### 3.2 参数说明

* **jdbcUrl**

	* 描述：目的数据库的 JDBC 连接信息。作业运行时，DataX 会在你提供的 jdbcUrl 后面追加如下属性：yearIsDateType=false&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true

               注意：1、在一个数据库上只能配置一个 jdbcUrl 值。这与 MysqlReader 支持多个备库探测不同，因为此处不支持同一个数据库存在多个主库的情况(双主导入数据情况)
                    2、jdbcUrl按照Mysql官方规范，并可以填写连接附加控制信息，比如想指定连接编码为 gbk ，则在 jdbcUrl 后面追加属性 useUnicode=true&characterEncoding=gbk。具体请参看 Mysql官方文档或者咨询对应 DBA。


 	* 必选：是 <br />

	* 默认值：无 <br />

* **username**

	* 描述：目的数据库的用户名 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **password**

	* 描述：目的数据库的密码 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **table**

	* 描述：目的表的表名称。支持写入一个或者多个表。当配置为多张表时，必须确保所有表结构保持一致。

               注意：table 和 jdbcUrl 必须包含在 connection 配置单元中

	* 必选：是 <br />

	* 默认值：无 <br />

* **column**

	* 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。如果要依次写入全部列，使用*表示, 例如: "column": ["*"]。

			**column配置项必须指定，不能留空！**

               注意：1、我们强烈不推荐你这样配置，因为当你目的表字段个数、类型等有改动时，你的任务可能运行不正确或者失败
                    2、 column 不能配置任何常量值

	* 必选：是 <br />

	* 默认值：否 <br />

* **session**

	* 描述: DataX在获取Mysql连接时，执行session指定的SQL语句，修改当前connection session属性

	* 必须: 否

	* 默认值: 空

* **preSql**

	* 描述：写入数据到目的表前，会先执行这里的标准语句。如果 Sql 中有你需要操作到的表名称，请使用 `@table` 表示，这样在实际执行 Sql 语句时，会对变量按照实际表名称进行替换。比如你的任务是要写入到目的端的100个同构分表(表名称为:datax_00,datax01, ... datax_98,datax_99)，并且你希望导入数据前，先对表中数据进行删除操作，那么你可以这样配置：`"preSql":["delete from 表名"]`，效果是：在执行到每个表写入数据前，会先执行对应的 delete from 对应表名称 <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **postSql**

	* 描述：写入数据到目的表后，会执行这里的标准语句。（原理同 preSql ） <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **writeMode**

	* 描述：控制写入数据到目标表采用 `insert into` 或者 `replace into` 语句<br />

	* 必选：是 <br />

	* 默认值：insert <br />

* **batchSize**

	* 描述：一次性批量提交的记录数大小，该值可以极大减少DataX与Mysql的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况。<br />

	* 必选：否 <br />

	* 默认值：1024 <br />


### 3.3 类型转换

类似 MysqlReader ，目前 MysqlWriter 支持大部分 Mysql 类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出 MysqlWriter 针对 Mysql 类型转换列表:


| DataX 内部类型| Mysql 数据类型    |
| -------- | -----  |
| Long     |int, tinyint, smallint, mediumint, int, bigint, year|
| Double   |float, double, decimal|
| String   |varchar, char, tinytext, text, mediumtext, longtext    |
| Date     |date, datetime, timestamp, time    |
| Boolean  |bit, bool   |
| Bytes    |tinyblob, mediumblob, blob, longblob, varbinary    |

 * `bit类型目前是未定义类型转换`


A: DataX 导入过程存在三块逻辑，pre 操作、导入操作、post 操作，其中任意一环报错，DataX 作业报错。由于 DataX 不能保证在同一个事务完成上述几个操作，因此有可能数据已经落入到目标端。

***

**Q: 按照上述说法，那么有部分脏数据导入数据库，如果影响到线上数据库怎么办?**

A: 目前有两种解法，第一种配置 pre 语句，该 sql 可以清理当天导入数据， DataX 每次导入时候可以把上次清理干净并导入完整数据。第二种，向临时表导入数据，完成后再 rename 到线上表。

***

**Q: 上面第二种方法可以避免对线上数据造成影响，那我具体怎样操作?**

A: 可以配置临时表导入
