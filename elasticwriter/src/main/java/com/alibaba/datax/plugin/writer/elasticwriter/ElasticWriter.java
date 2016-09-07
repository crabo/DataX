
package com.alibaba.datax.plugin.writer.elasticwriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.ParseException;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.fastjson.JSON;

public class ElasticWriter extends Writer {
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            List<String> hosts = this.originalConfig.getList(EsKey.HOST, String.class);
            if(hosts==null || hosts.isEmpty() || hosts.get(0).indexOf(':')<0)
            throw DataXException.asDataXException(
                    DBUtilErrorCode.ILLEGAL_VALUE, "host is required as ['localhost:9200','...']\n"
                    		+this.originalConfig.beautify());
            
            if(StringUtils.isBlank(this.originalConfig.getString(EsKey.INDEX))){
            	 throw DataXException.asDataXException(
                         DBUtilErrorCode.ILLEGAL_VALUE, "Es 'index' name is require.\n"
                        		 +this.originalConfig.beautify());
            }
            if(StringUtils.isBlank(this.originalConfig.getString(EsKey.DOCUMENT))){
            	 throw DataXException.asDataXException(
                         DBUtilErrorCode.ILLEGAL_VALUE, "Es 'document' type is require.\n"
                        		 +this.originalConfig.beautify());
            }
        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                writerSplitConfigs.add(this.originalConfig);
            }

            return writerSplitConfigs;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);
        
        private Configuration writerSliceConfig;
        protected int batchSize;
        protected int batchByteSize;
        protected String writeMode;
        protected boolean parseArray;
        protected List<String> columns;
        protected int columnNumber;
        
        private String index;
        private String document;
        private int dateField;
        private int MONTH_PER_SHARD;//如每3个月合并为一个shard,一年则有4个分片
        protected HttpHost[] hosts;
        private Map<String, String> REQUEST_PARAMS;



        @Override
        public void init() {
            this.writerSliceConfig = getPluginJobConf();
            
            List<String> hostList = this.writerSliceConfig.getList(EsKey.HOST, String.class);
            
            List<HttpHost> li=new ArrayList<HttpHost>(hostList.size());
        	for(String h : hostList)
        		li.add(HttpHost.create(h));
        	this.hosts = li.toArray(new HttpHost[0]);
            
            this.index = this.writerSliceConfig.getString(EsKey.INDEX);
            this.document = this.writerSliceConfig.getString(EsKey.DOCUMENT);
            //按照日期字段选择indices分库
            this.dateField = this.writerSliceConfig.getInt(EsKey.DATE_FIELD,-1);
            this.MONTH_PER_SHARD = this.writerSliceConfig.getInt(EsKey.MONTH_PER_SHARD,3);
            
            
            this.batchSize = writerSliceConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
            this.batchByteSize = writerSliceConfig.getInt(Key.BATCH_BYTE_SIZE, Constant.DEFAULT_BATCH_BYTE_SIZE);
            this.writeMode = writerSliceConfig.getString(Key.WRITE_MODE, "update");//index 或  update
            this.columns = writerSliceConfig.getList(Key.COLUMN, String.class);
            this.parseArray= writerSliceConfig.getBool("parseArray", false);
            this.columnNumber = this.columns.size();
            
            REQUEST_PARAMS=new HashMap<String,String>(0);//EMPTY
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
        	RestClient client = RestClient.builder(this.hosts)
        			//.setHttpClientConfigCallback(b -> b.setDefaultHeaders(
        	        //        Collections.singleton(new BasicHeader(HttpHeaders.ACCEPT_ENCODING, "gzip"))))
    	            //.setRequestConfigCallback(b -> b.setContentCompressionEnabled(true))
        		.build();
        	
        	startWriteWithConn(recordReceiver,client);
        }
        
        private void startWriteWithConn(RecordReceiver recordReceiver,RestClient conn){
        	List<Record> writeBuffer = new ArrayList<Record>(this.batchSize);
    		int bufferBytes = 0;
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    writeBuffer.add(record);
                    bufferBytes += record.getMemorySize();

                    if (writeBuffer.size() >= batchSize || bufferBytes >= batchByteSize) {
                    	doBulkInsert(conn, writeBuffer);
                        writeBuffer.clear();
                        bufferBytes = 0;
                    }
                }
                
                if (!writeBuffer.isEmpty()) {
                	doBulkInsert(conn, writeBuffer);
                    writeBuffer.clear();
                    bufferBytes = 0;
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                writeBuffer.clear();
                bufferBytes = 0;
                
                try {
                	if(conn!=null)
                		conn.close();
                	conn=null;
				} catch (IOException e) {
					LOG.info("ElasticSearch RestClient error on close",e);
				}
            }
        }
        private void doBulkInsert(RestClient conn,List<Record> records) throws IOException{
        	StringBuilder sb = new StringBuilder();
        	for(Record r : records)
        		appendBulk(sb,r,getNestedDoc(r));
        	postRequest(conn,sb);
        }
        
        protected void postRequest(RestClient conn,StringBuilder sb) throws IOException
        {
        	HttpEntity entity = new NStringEntity(sb.toString(), ContentType.APPLICATION_JSON);

        	Response resp = conn.performRequest("POST", "/_bulk", this.REQUEST_PARAMS, entity);
        	if(failInBulk(resp)){
        		//throw new IllegalArgumentException("_bulk post failed");
        	}
        	
        	/*
        	conn.performRequest("POST", "/_bulk", this.REQUEST_PARAMS, entity, new ResponseListener(){
				@Override
				public void onSuccess(Response resp) {
					try {
						if(failInBulk(resp)){
							
						}
					} catch (ParseException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}

				@Override
				public void onFailure(Exception ex) {
					LOG.warn("ElasticSearch '_bulk' post failed, exception=\r\n{}",ex);
				}
        		
        	});*/
        }
        private boolean failInBulk(Response resp) throws ParseException, IOException{
        	String result = EntityUtils.toString(resp.getEntity(), StandardCharsets.UTF_8);
        	if(resp.getStatusLine().getStatusCode()>HTTP_STATUS_OK
        			|| result.indexOf("\"errors\":true")>0)
        	{
        		int i = result.indexOf("\"error\":");
        		result = result.substring(i, result.indexOf('}',i));
        		
        		LOG.warn("ElasticSearch '_bulk' post failed, first error=\r\n{}",result);
        		return true;
        	}
        	return false;
        }
        static int HTTP_STATUS_OK=201;
        
        protected void appendBulk(StringBuilder sb,Record rMeta,Map<String,Object> record)
        {
        	/*
        	{ "index" : { "_index" : "test", "_type" : "type1", "_id" : "1" } }
        	{ "field1" : "value1" }
        	{ "update" : {"_id" : "1", "_type" : "type1", "_index" : "index1"} }
        	{ "doc" : {"field2" : "value2"} }
        	{ "update" : {"_id" : "1", "_type" : "type1", "_index" : "index1"} }
        	{ "doc" : {"field2" : "value2"} , "doc_as_upsert" : true}
        	 */
        	//action&meta
        	sb.append("{\"").append(this.writeMode).append("\":");
        		appendMeta(sb,rMeta);
        	sb.append("}\n");
        	
        	//data
        	if("update".equals(this.writeMode)){
        		sb.append("{\"doc\":");
        		sb.append(JSON.toJSONString(record));
        		sb.append(",\"doc_as_upsert\":true}");
        	}else{
        		sb.append(JSON.toJSONString(record));
        	}
        	sb.append("\n");
        }
        protected void appendMeta(StringBuilder sb,Record r){
        	String idx = this.index;
        	if(this.dateField>-1){
        		idx = idx.replace("%%", 
        				getShardPattern(r.getColumn(this.dateField).asDate())
        			);
        	}
        	
        	sb.append("{\"_index\":\"").append(idx)
        		.append("\",\"_type\":\"").append(this.document)
        		.append("\"");
        		//.append("\",\"_id\":\"").append(r.getColumn(this.idField).getRawData());
        		
        	
        	for(int i=0;i<this.columnNumber;i++){
        		if(i==this.dateField) continue;//分片控制字段不进入meta
        		
        		String colName = this.columns.get(i);
        		if(!colName.startsWith("_"))//连续的下划线字段，将一一进入meta
        			break;
        		else{
        			sb.append(",\"").append(colName).append("\":\"")
        				.append(r.getColumn(i).getRawData())
        				.append("\"");
        		}
        	}
        	sb.append("}");
        }
        
        private Map<String,Object> getNestedDoc(Record r){
        	Map<String,Object> root = new HashMap<String,Object>();
        	for(int i=0;i<this.columnNumber;i++){
        		String colName = this.columns.get(i);
        		if(!colName.startsWith("_"))//所有下划线开头的字段都忽略
        			appendNestedProp(root,colName,r.getColumn(i));
        	}
        	return root;
        }
        
        protected final String NESTED_SPLITTER=".";
        //以'.'号分隔的字段名， 要转为多层嵌套对象
        protected void appendNestedProp(Map<String,Object> root,String props,Column val){
        	if(props.indexOf(NESTED_SPLITTER)>0){
	        	String[] nested = StringUtils.split(props, NESTED_SPLITTER, 2);//第一个分隔符
	    		Map<String,Object> child = (Map<String,Object>)root.get(nested[0]);
				if(child==null){
					child=new HashMap<String,Object>();
					root.put(nested[0], child);
				}
				
				appendNestedProp(child,nested[1],val);
        	}
        	else{
        		setColumValue(root,props,val);
        	}
        }
        
        protected void setColumValue(Map<String,Object> obj,String props,Column val)
        {
        	if(val instanceof DateColumn)//日期型
        		obj.put(props, 
    					null == val.getRawData()?null:
    					DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT//2009-03-20T22:07:01+08:00
    						.format(val.asDate())
    					);
    		else
    			obj.put(props, val.getRawData());
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

        int _prevMonth,_prevShard;//flyweight pattern
        /**
         * 2016-01-01 返回 1601
         */
        private String getShardPattern(Date dt){
        	Calendar calc = Calendar.getInstance();
        	calc.setTime(dt);
        	
    		int m=calc.get(Calendar.MONTH);
    		if(_prevMonth!=m){
    			_prevShard = (int) (Math.ceil((m-1)/this.MONTH_PER_SHARD)+1);
    			_prevMonth=m;
    		}
    		
    		Integer y = calc.get(Calendar.YEAR);
    		return y.toString().substring(2)+String.format("%02d", _prevShard);
        }
    }
}
