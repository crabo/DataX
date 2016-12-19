
package com.alibaba.datax.plugin.writer.elasticwriter;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.datax.plugin.writer.elasticwriter.ElasticArrayWriter.Task;
import com.alibaba.fastjson.JSON;

public class JsonWriter extends Writer {
    public static class Job extends ElasticWriter.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
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

    public static class Task extends ElasticWriter.Task {
    	private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);
        private int ID_FIELD=0;
        private boolean isCanRead=true;//当read()返回null时，再次read()将阻塞进程。
        int BUFFER_SIZE=1000000;//1MB
        
        @Override
        public void startWrite(RecordReceiver recordReceiver) {
        	BufferedOutputStream fs = null;
			try {
				fs = new BufferedOutputStream(
						new FileOutputStream("es_data.json")
	        	        //new GZIPOutputStream(new FileOutputStream("es_data.json.gz"))
	        	        ,BUFFER_SIZE);
				
				this.isCanRead=true;
        		startWriteWithConn(recordReceiver,fs);
			} catch (IOException e) {
				LOG.warn("open file 'es_data.json' error",e);
			}finally{
				LOG.info("close file 'es_data.json'");
				if(fs!=null)
					try {
						fs.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
			}
        }
        
        private void startWriteWithConn(RecordReceiver recordReceiver,BufferedOutputStream conn){
        	List<Record> writeBuffer = new ArrayList<Record>(this.batchSize+30);//合并末尾的同一分组
    		int bufferBytes = 0;
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    writeBuffer.add(record);
                    bufferBytes += record.getMemorySize();

                    if (writeBuffer.size() >= batchSize || bufferBytes >= batchByteSize) {
                    	record = trySplitGroupById(recordReceiver,writeBuffer);
                    	doBulkInsert(conn,this.index, writeBuffer);
                        super.afterBulk(writeBuffer);
                        bufferBytes = 0;
                        
                        if(record!=null)//下一batch的第一条记录
                        {
                        	writeBuffer.add(record);
                        }
                        if(isCanRead==false)
                        	break;
                    }
                }
                if (!writeBuffer.isEmpty()) {
                	doBulkInsert(conn,this.index, writeBuffer);
                    super.afterBulk(writeBuffer);
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
        
        private void doBulkInsert(BufferedOutputStream conn,String idx,List<Record> records) throws IOException{
        	StringBuilder sb = new StringBuilder();
        	
        	Record[] array = records.toArray(new Record[0]);
        	Map<String,List<Record>> groups = splitArrayToGroup(array);
        	for(Record r : array)
        	{
        		if(r!=null)
        			super.appendBulk(sb,idx,r,getNestedDoc(r));
        	}
        		
        	appendArrayProp(sb,idx,groups);
        	conn.write(sb.toString().getBytes("utf-8"));
        }
        
        /**
         * 是否reader正常
         * @return 不属于同组的第一条记录
         */
        Record trySplitGroupById(RecordReceiver recordReceiver,List<Record> writeBuffer){
        	if(!writeBuffer.isEmpty())
        	{
        		Record prev = writeBuffer.get(writeBuffer.size()-1);
        		String prevId= prev.getColumn(this.ID_FIELD).asString();
        		 Record record;
                 while ((record = recordReceiver.getFromReader()) != null) {
                	 if(prevId.equals(record.getColumn(this.ID_FIELD).asString())){
                		 writeBuffer.add(record);
                	 }else{//diff group
                		 return record;
                	 }
                 }
                 if(record==null)
                	 this.isCanRead = false;
        	}
        	return null;
        }
       
        
        final String ARRAY_SPLITTER="[";
        void appendArrayProp(StringBuilder sb,String idx,Map<String,List<Record>> groups)
        {
        	for(Entry<String, List<Record>> group : groups.entrySet())
        	{
        		Record r = group.getValue().get(0);
        		Map<String,Object> root = getNestedDoc(r);
        		appendArrayPropsOnly(root,group.getValue());
        		
        		super.appendBulk(sb,idx,r,root);
        	}
        }
        
        Map<String,Object> getNestedDoc(Record r){
        	Map<String,Object> root = new HashMap<String,Object>();
        	
        	for(int i=0;i<this.columnNumber;i++){
        		String colName = this.columns.get(i);
        		if(!colName.startsWith("_"))//所有下划线开头的字段都忽略
        		{
        			if(colName.indexOf(ARRAY_SPLITTER)<0)
        				appendNestedProp(root,colName,r.getColumn(i));
        			else
        				appendNestedArrayProp(root,colName,r.getColumn(i));
        		}
        	}
        	return root;
        }
        
        /**
         * 合并第一条之后的所有数组到当前root
         */
        void appendArrayPropsOnly(Map<String,Object> root, List<Record> records){
        	for(int k=1;k<records.size();k++)//跳过第一条
        	{
        		Record r = records.get(k);
        		
	        	for(int i=0;i<this.columnNumber;i++){
	        		String colName = this.columns.get(i);
	        		if(colName.indexOf(ARRAY_SPLITTER)>0){
	        			appendNestedArrayProp(root,colName,r.getColumn(i));
	        		}
	        	}
        	}
        }
        /**
         * 对数组的属性使用单独的赋值方式
         */
        void appendNestedArrayProp(Map<String,Object> root,String props,Column val){
        	if(props.indexOf(NESTED_SPLITTER)>0){
	        	String[] nested = StringUtils.split(props, NESTED_SPLITTER, 2);//第一个分隔符
	    		Map<String,Object> child = (Map<String,Object>)root.get(nested[0]);
				if(child==null){
					child=new HashMap<String,Object>();
					root.put(nested[0], child);
				}
				
				appendNestedArrayProp(child,nested[1],val);
        	}
        	else{//order[itemID]
        		String[] array = StringUtils.split(props, ARRAY_SPLITTER);
        		List<Map<String,Object>> list = (List<Map<String,Object>>)root.get(array[0]);
				if(list==null){
					list=new ArrayList<Map<String,Object>>();//构建一个初始数组
					root.put(array[0], list);
				}
				
				String column=array[1].replace("]", "");
				Map<String,Object> child =null;
				if(list.isEmpty() || list.get(list.size()-1).containsKey(column))//列名重复，则是新的一条记录
				{
					child=new HashMap<String,Object>();
					list.add(child);
				}else
					child=list.get(list.size()-1);//旧的一条记录
				
				super.setColumValue(child, column ,val);
        	}
        }
        
        /**
         * 将数组内同组的记录移动到Map<>中， 原数组值置为null
         * @param records 不存在分组的记录
         * @return 存在分组的记录
         */
        Map<String,List<Record>> splitArrayToGroup(Record[] records){
        	Map<String,List<Record>> groups = new HashMap<String,List<Record>>();
        	if(records.length>1){
	        	String prevId=records[0].getColumn(ID_FIELD).asString();
	        	for(int i=1;i<records.length;i++)
	    		{
	        		String id = records[i].getColumn(ID_FIELD).asString();
	        		if(id.equals(prevId)){
	        			List<Record> g = groups.get(id);
	        			if(g==null){
	        				g=new ArrayList<Record>();
	        				g.add(records[i-1]);//prev one
	        				records[i-1]=null;
	        				
	        				groups.put(id, g);//move record to Map<>
	        			}
	        			g.add(records[i]);//curent
	        			records[i]=null;//drop this
	        		}else
	        			prevId=id;
	    		}
        	}
        	return groups;
        }
    }
}
