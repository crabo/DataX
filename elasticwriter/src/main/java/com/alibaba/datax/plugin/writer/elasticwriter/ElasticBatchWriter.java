
package com.alibaba.datax.plugin.writer.elasticwriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;

public class ElasticBatchWriter extends Writer {
    public static class Job extends ElasticArrayWriter.Job {
    	
    	private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);
    	/**
    	 * Map<database, <column,value>>
    	 */
    	public static Map<String,Map<String,String>> ES_SERVERS;
    	public static MysqlConfigure MONITOR_CENTER;
    	
    	@Override
        public void init() {
    		super.init();
    		MONITOR_CENTER=new MysqlConfigure();
    	}
    	
    	@Override
		public void prepare() {
    		//resultset: [dbName, hostUrl]
    		List<Map<String,String>> li = MONITOR_CENTER.getConfig();
    		
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
    	private int DATABASE_FIELD;//当前记录所属database
    	private int UPDATE_TIME_FIELD;//记录最后更新时间，跟踪执行进度
    	
        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);
        
        
        @Override
        public void init() {
        	super.init();
        	
        	//批量reader读取记录时，$database字段存储在最后一个StringColumn中
        	DATABASE_FIELD = super.columnNumber;
        	UPDATE_TIME_FIELD = this.writerSliceConfig.getInt(EsKey.UPDATE_TIME_FIELD,-1);
        }
        
        static Map<String,RestClient> ClientHolder=new HashMap<String,RestClient>();
        @Override
        protected RestClient getClient(String host){
        	if(!ClientHolder.containsKey(host))
        	{
        		synchronized (ClientHolder){
	        		ClientHolder.put(host, 
	        				this.createClient(host)
	        				);
        		}
        	}
        	return ClientHolder.get(host);
        }
        @Override
        public void startWrite(RecordReceiver recordReceiver){
        	
        	List<Record> writeBuffer = new ArrayList<Record>(this.batchSize+30);//合并末尾的同一分组
        	String prevDb=null;
        	try {
                Record record;
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
            	if(this.UPDATE_TIME_FIELD>-1 && prevDb!=null){
					Job.MONITOR_CENTER.updateError(
							prevDb,e);
				}
            	
                throw DataXException.asDataXException(
                		EsErrorCode.ERROR, e);
            } finally {
                writeBuffer.clear();
            }
        }
        
        private void doInsertByDatabase(String database,List<Record> writeBuffer) throws InterruptedException,IOException{
        	Map<String,String> es = Job.ES_SERVERS.get(database);
        	
        	if(es!=null){//Es server not deployed yet?
        		if(LOG.isDebugEnabled()){
            		LOG.debug("task[{}] writing '{}'({}) docs to {} ",this.getTaskId(),writeBuffer.size(),
            				database,(es.get("host")+"/"+es.get("index")) );
            	}
        		
				doBulkInsert(
						this.getClient(es.get("host"))
						,es.get("index"), writeBuffer);
				
				if(this.UPDATE_TIME_FIELD>-1){
					Job.MONITOR_CENTER.updateProgress(
							database,
							writeBuffer
								.get(writeBuffer.size()-1)//last row
								.getColumn(this.UPDATE_TIME_FIELD)
								.asDate()
							);
				}
        	}else{
        		LOG.warn("task[{}] writing '{}'({}) docs to host 'NULL' ",this.getTaskId(),writeBuffer.size(),database);
        	}
        	
        	super.afterBulk(writeBuffer);
        }
        
        @Override
        public void destroy() {
        	for(RestClient c: ClientHolder.values()){
        		try {
					c.close();
				} catch (IOException e) {
				}
        	}
        	ClientHolder.clear();
        }
    }
}
