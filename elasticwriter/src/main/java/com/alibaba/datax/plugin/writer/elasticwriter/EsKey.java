package com.alibaba.datax.plugin.writer.elasticwriter;

public class EsKey {
    public static final String HOST = "host";//["localhost:9200","nascent:9300"]

    public static final String INDEX = "index";//crm-%% 自动替换百分号为日期分片值
    public static final String INDEX_CREDENTIAL = "index_auth";//用户名:密码

    public static final String DOCUMENT = "document";

    public static final String ID_FIELD = "id_field";//SellerNick所在的字段顺序，默认0
    public static final String DATE_FIELD = "date_field";//CreateTime所在的字段顺序，默认-1
    public static final String UPDATE_TIME_FIELD = "update_time_field";//UpdateTime所在的字段顺序，默认-1

    public static final String MONTH_PER_SHARD = "month_per_shard";//3, 一年中每3个月合并到一个分片

}
