# DataX ElasticWriter


---


## 1 快速介绍

ElasticWriter 插件实现了写入数据到 ElasticSearch 主库的目的表的功能。
根据writeMode ="index"/"update" ，可以控制是记录导入， 或是局部更新doc。
同时根据"index":"crm-%%" 的%%占位符， 和date_field对应的column位置， 自动计算记录所属的分库，
实现数据导入分库的效果：如 crm-1602

column[] 中，所有下划线开头的字段都忽略，如 "_id","_date" 等将不导入到es。

内部实现使用ElasticSearch5 的RestClient 直接提交POST请求到_bulk 接口。



## 2 实现原理

VerticaWriter 通过 DataX 框架获取 Reader 生成的协议数据，根据你配置的 `writeMode` 提交到_bulk
writeMode=index时
{ "index" : { "_index" : "test", "_type" : "type1", "_id" : "1" } }
{ "field1" : "value1" }

writeMode=update时
{ "update" : {"_id" : "1", "_type" : "type1", "_index" : "index1"} }
{ "doc" : {"field2" : "value2"} , "doc_as_upsert" : true}



## 3 功能说明

### 3.1 配置样例

* 这里使用一份从内存产生到 Mysql 导入的数据。

```json
{
    "job": {
        "setting": {
        	"ts_adjustnow_sec":-10, //修正mysql和本机now时间的误差： ts_start=now()+ (正负的adjust_sec) 
            "ts_batch_days":30, //不设置，则ts_end=now(), 设置时： ts_end=ts_start+days
            "ts_interval_sec":15, //设置则启用task无限循环执行模式，必须配合delta增量配置
            "ts_file":"verticajob.ts.txt", //启用delta增量配置使用的持久时间戳:任务启动时间
            "speed": {
                "channel":1   //逐个作业排队执行
            },
            "errorLimit": {
                "record": 0,
                "percentage":0.02
            }
        },
        "content": [
           {
            "reader": {
                "name": "mysqlreader",
                "parameter": {
                    "username": "jusrut7et7de",
                    "password": "*****",
                    "connection": [{  //注意此处各项参数均为数组
                        "querySql": ["SELECT ID AS id,Nick AS name,CreateTime AS create_date FROM kd_customer where CreateTime>='$ts_start' and CreateTime<'$ts_end' order by CreateTime limit $limit"],
                        "jdbcUrl": ["jdbc:mysql://localhost:3306/test"]
                    }]
                }
            },"writer": {
                "name": "elasticwriter",
                "parameter": {
        			"batchSize":1000,
					"writeMode": "index", //替换整条记录。 update则部分更新到记录（或新增）
					"index":"crm-%%",    //%%会替换为对应的分库值：1603
					"document":"customer",
					"date_field":2,      //用于分库的字段在column中的位置
					"month_per_shard":6, //基于日期的分库：每6月一个分片。 则一年共计2个indices
					"column": ["_id",    //column中连续的下划线变量为meta值：_id,_parent,_routing
						 "_parent",
						 "name",
						 "create_date",
						 "member.grade"
						 ],
					"host":["114.215.171.188:9200"] //RestClient支持多host处理
                }
            }
        }
        
        ,{"taskId":1,  ###继续增加其余table的配置项。
           "reader":{....}
           "writer":{.....}
         }
   }

```

