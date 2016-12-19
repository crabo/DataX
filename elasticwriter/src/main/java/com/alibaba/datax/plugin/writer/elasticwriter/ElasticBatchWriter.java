
package com.alibaba.datax.plugin.writer.elasticwriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
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

import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.apache.http.HttpHost;
import org.apache.http.ParseException;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

import com.alibaba.fastjson.JSON;

public class ElasticBatchWriter extends Writer {
    public static class Job extends ElasticArrayWriter.Job {
    	
    	private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);
    	/**
    	 * Map<database, <column,value>>
    	 */
    	public static Map<String,Map<String,String>> ES_SERVERS;
    	@Override
        public void init() {
    		super.init();
    		//resultset: [dbName, hostUrl]
    		List<Map<String,String>> li = new MysqlConfigure().getConfig();
    		
    		ES_SERVERS=new HashMap<>(li.size());
    		for(Map<String,String> cfg : li){	
    			if(cfg.get("hostUrl")!=null && cfg.get("dbName")!=null)
    			{
	    			regexProcess(cfg);
	    			ES_SERVERS.put(cfg.get("dbName"),cfg);
    			}
    		}
        	
        	if(LOG.isDebugEnabled())
        	{
        		LOG.debug("=========Es 10/{} host settings=========",Job.ES_SERVERS.size());
        		int i=0;
	        	for(Entry<String,Map<String,String>> entry : Job.ES_SERVERS.entrySet()){
	        		
	        		LOG.debug("dbName '{}' => {}/{}",entry.getKey(),
	        				entry.getValue().get("host"),
	        				entry.getValue().get("index"));
	        		if(i>10) break;
	        		i++;
	        	}
        	}
    	}
    	
    	//match http://10.26.233.236:9200/crm-xinshiqjd-*/customer/_userdiv
    	//  10.26.233.236  :   9200   crm-xinshiqjd-*
    	Pattern p = Pattern.compile("http://(.*):(\\d+)/(.*?)/");
    	void regexProcess(Map<String,String> cfg){
    		Matcher matcher = p.matcher(cfg.get("hostUrl"));
		    if(matcher.find())
		    {
			    cfg.put("host",String.format("%s:%s", matcher.group(1),matcher.group(2)));
			    cfg.put("index", matcher.group(3).replace("*", "%%"));
		    }
    	}
    }

    public static class Task extends ElasticArrayWriter.Task {
    	private int DATABASE_FIELD;
        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);
        
        @Override
        public void init() {
        	super.init();
        	
        	//批量reader读取记录时，$database字段存储在最后一个StringColumn中
        	DATABASE_FIELD = super.columnNumber;
        }
        @Override
        public void startWrite(RecordReceiver recordReceiver){
        	RestClient conn = null;
        	
        	List<Record> writeBuffer = new ArrayList<Record>(this.batchSize+30);//合并末尾的同一分组
            try {
                Record record;
                String prevDb=null;
                while ((record = recordReceiver.getFromReader()) != null) {
                	if(record.getColumn(this.DATABASE_FIELD).asString()
                			.equals(prevDb))
                	{
	                    writeBuffer.add(record);
	
	                    if (writeBuffer.size() >= batchSize) {
	                    	record = trySplitGroupById(recordReceiver,writeBuffer);
	                    	doInsertByDatabase(prevDb, writeBuffer);
	                        
	                        if(record!=null)//下一batch的记录被取出了
	                        {
	                        	writeBuffer.add(record);//已经flush过，可以安全加入为buffer的第一条
	                        	prevDb=record.getColumn(this.DATABASE_FIELD).asString();//可能变更，也可能未变
	                        }
	                        if(isCanRead==false)
	                        	break;
	                    }
                	}else{//每次切换database，无须判断再group， 直接post数据
                		if(writeBuffer.size()>0){//buffer与当前record不同database
                			doInsertByDatabase(prevDb, writeBuffer);
                		}
                		writeBuffer.add(record);//已经flush过，可以安全加入为buffer的第一条
                		prevDb=record.getColumn(this.DATABASE_FIELD).asString();
                	}
                }
                if (!writeBuffer.isEmpty()) {
                	doInsertByDatabase(prevDb, writeBuffer);
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                writeBuffer.clear();
            }
        }
        
        private void doInsertByDatabase(String database,List<Record> writeBuffer) throws IOException{
        	Map<String,String> es = Job.ES_SERVERS.get(database);
        	
        	if(es!=null){//Es server not deployed yet?
        		if(LOG.isDebugEnabled()){
            		LOG.debug("task[{}] writing '{}'({}) docs to {} ",this.getTaskId(),writeBuffer.size(),
            				database,(es.get("host")+"/"+es.get("index")) );
            	}
        		
				doBulkInsert(super.newClient(es.get("host")),es.get("index"), writeBuffer);
        	}else{
        		LOG.warn("task[{}] writing '{}'({}) docs to host 'NULL' ",this.getTaskId(),writeBuffer.size(),database);
        	}
        	
        	super.afterBulk(writeBuffer);
        }
        
    }
}
