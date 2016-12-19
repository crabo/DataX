
package com.alibaba.datax.plugin.writer.elasticwriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.ParseException;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
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
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class ElasticWriter extends Writer {
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        protected Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            List<String> hosts = this.originalConfig.getList(EsKey.HOST, String.class);
            if(hosts==null || hosts.isEmpty())
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
        
        protected Configuration writerSliceConfig;
        protected int batchSize;
        protected int batchByteSize;
        protected String writeMode;
        protected boolean parseArray;
        protected List<String> columns;
        protected int columnNumber;
        
        private String indexCredential;
        protected String index;
        private String document;
        private int dateField;
        private int MONTH_PER_SHARD;//如每3个月合并为一个shard,一年则有4个分片
        //protected HttpHost[] hosts;
        protected List<String> hostList;
        private Map<String, String> REQUEST_PARAMS;

        static Map<String,RestClient> ClientHolder=new HashMap<String,RestClient>();
        BasicHeader _credentialHeader;
        BasicHeader getCredential(){
        	if(_credentialHeader==null)
        	{
        		if(indexCredential!=null)
        		{
	        		//格式为  用户名：密码
	        		byte[] credentials = Base64.encodeBase64(indexCredential.getBytes(StandardCharsets.UTF_8));
	        	
	        		_credentialHeader = new BasicHeader("Authorization","Basic " 
	    				+ new String(credentials, StandardCharsets.UTF_8));
        		}else
        			_credentialHeader=new BasicHeader("User-Agent","Nascent-SDK");
        	}
        	return _credentialHeader;
        }

        @Override
        public void init() {
            this.writerSliceConfig = getPluginJobConf();
            
            hostList = this.writerSliceConfig.getList(EsKey.HOST, String.class);
            
            //List<HttpHost> li=new ArrayList<HttpHost>(hostList.size());
        	//for(String h : hostList)
        	//	li.add(HttpHost.create(h));
        	//this.hosts = li.toArray(new HttpHost[0]);
            
            this.index = this.writerSliceConfig.getString(EsKey.INDEX);
            this.indexCredential = this.writerSliceConfig.getString(EsKey.INDEX_CREDENTIAL);
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
        
        RestClient newClient(String host){
        	if(!ClientHolder.containsKey(host))
        	{
        		synchronized (ClientHolder){
	        		ClientHolder.put(host, 
	    				RestClient.builder(new HttpHost[]{HttpHost.create(host)})
	    				.setMaxRetryTimeoutMillis(30000)
	            			//.setHttpClientConfigCallback(b -> b.setDefaultHeaders(
	            	        //        Collections.singleton(new BasicHeader(HttpHeaders.ACCEPT_ENCODING, "gzip"))))
	        	            //.setRequestConfigCallback(b -> b.setContentCompressionEnabled(true))
	            		.build()
	        				);
        		}
        	}
        	return ClientHolder.get(host);
        }
        @Override
        public void startWrite(RecordReceiver recordReceiver) {
        	
        	startWriteWithConn(recordReceiver,newClient(this.hostList.get(0)));
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
                    	afterBulk(writeBuffer);
                        bufferBytes = 0;
                    }
                }
                
                if (!writeBuffer.isEmpty()) {
                	doBulkInsert(conn, writeBuffer);
                	afterBulk(writeBuffer);
                    bufferBytes = 0;
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                writeBuffer.clear();
                bufferBytes = 0;
                conn=null;
            }
        }
        private void doBulkInsert(RestClient conn,List<Record> records) throws IOException{
    		postRequest(conn,this.index,records);
        }
        
        protected StringBuilder recordToDoc(String idx,List<Record> records){
        	StringBuilder sb = new StringBuilder();
        	for(Record r : records)
        		appendBulk(sb,idx,r,getNestedDoc(r));
        	return sb;
        }
        protected void afterBulk(List<Record> writeBuffer){
        	writeBuffer.clear();
        	//this.indices.clear();  use with updateRefresh()
        }
        
        private HttpEntity getPostEntity(String idx,List<Record> records,Map<String,String> fails){
        	if(fails!=null){
        		for(int i=records.size()-1;i>=0;i--){
        			if(!fails.containsKey(records.get(i).getColumn(0).asString()))
        				records.remove(i);
        		}
        	}
        	StringBuilder sb = recordToDoc(idx,records);
        	return new NStringEntity(sb.toString(), ContentType.APPLICATION_JSON);
        }
        protected void postRequest(RestClient conn,String idx,List<Record> records) throws IOException
        {
        	int retries=1;
        	Map<String,String> fails=null;
        	do{
	        	HttpEntity entity = getPostEntity(idx,records,fails);
	        	try{
	        		fails = doPost(conn,entity);
		        }catch(RuntimeException e){
					if(e.getCause()!=null && e.getCause() instanceof TimeoutException){
						try {
							Thread.sleep(3000*retries);
						} catch (InterruptedException e1) {
						}
						LOG.warn("ElasticSearch RestClient timeout-error on request, {}# {} retring",retries,idx);
					}else
						throw e;
				}catch(IOException ex){
		    		try {
            			if(retries>10){//NETWORK ERROR?
            				LOG.warn("ElasticSearch RestClient IO-error too many times, low down 'batchSize' setting please!");
            				Thread.sleep(1000*2^(retries-10));
            			}
    					Thread.sleep(3000*retries);
	    				
					} catch (InterruptedException e) {
					}
		    		LOG.warn("ElasticSearch RestClient IO-error on request, {}# {} retring \n {}",retries,idx,ex);
		    	}
	        	retries++;
    		}while(fails!=null && retries<18);
        	
        	if(fails!=null){
        		LOG.error("========failed bulk docs=======\n{}",fails);
        	}
        }
        private Map<String,String> doPost(RestClient conn,HttpEntity entity)throws IOException{
        	Response resp = conn.performRequest("POST", "/_bulk", this.REQUEST_PARAMS, entity
        			,this.getCredential());
        	
        	return extractFailedInBulk(resp);
    		//throw new IllegalArgumentException("error occur on es ingrest,please check elasticsearch log for details!");
    	        
        }
        
        
        /**
         * Map<_id,_index>
         */
        private Map<String,String> extractFailedInBulk(Response resp) throws ParseException, IOException{
        	String result = EntityUtils.toString(resp.getEntity(), StandardCharsets.UTF_8);
        	
        	if(resp.getStatusLine().getStatusCode()>HTTP_STATUS_OK
        			|| result.indexOf("\"errors\":true")>0)
        	{
        		Map<String,String> failedBatch = new HashMap<>();
        		JSONObject obj = JSON.parseObject(result);
        		JSONArray items = obj.getJSONArray("items");
        		if(items!=null){
	        		for(int i=0;i<items.size();i++){
	        			obj = items
	        					.getJSONObject(i)
	        					.getJSONObject("index");
	        			
	        			if(obj.getIntValue("status")>HTTP_STATUS_OK){
	        				failedBatch.put(obj.getString("_id"),obj.getString("_index"));
	        			}
	        		}
	        		
	        		if(LOG.isTraceEnabled())
		        		LOG.trace("ElasticSearch '_bulk' post failed, error docs: \n{}",
		        				failedBatch
		        				);
        		}else{
        			LOG.debug("ElasticSearch '_bulk' post failed, errors: \n{}",
        					result
	        				);
        		}
        		
        		//should stop the job???
        		//throw new IllegalArgumentException("error occur on es ingrest,please check elasticsearch log for details!");
        		
        		return failedBatch;
        	}
        	return null;
        }
        static int HTTP_STATUS_OK=201;
        
        protected void appendBulk(StringBuilder sb,String idx,Record rMeta,Map<String,Object> record)
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
        		appendMeta(sb,idx,rMeta);
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
        protected void appendMeta(StringBuilder sb,String idx,Record r){
        	if(this.dateField>-1){
        		idx = idx.replace("%%", 
        				getShardPattern(r.getColumn(this.dateField).asDate())
        			);
        	}
        	//if(!indices.contains(idx))//每一个批次设计的index数目
        	//	indices.add(idx);
        	
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

        int _prevMonth=-1,_prevShard;//flyweight pattern
        /**
         * 2016-01-01 返回 1601
         */
        private String getShardPattern(Date dt){
        	if(dt==null){
        		return "0001";
        	}
        	Calendar calc = Calendar.getInstance();
        	calc.setTime(dt);
        	
    		int m=calc.get(Calendar.MONTH);//start form 0-11
    		if(_prevMonth!=m){
    			_prevShard = (int) (Math.ceil(m/this.MONTH_PER_SHARD)+1);
    			_prevMonth=m;
    		}
    		
    		Integer y = calc.get(Calendar.YEAR);
    		return y.toString().substring(2)+String.format("%02d", _prevShard);
        }
    }
}
