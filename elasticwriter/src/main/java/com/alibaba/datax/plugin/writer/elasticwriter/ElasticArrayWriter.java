
package com.alibaba.datax.plugin.writer.elasticwriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;

public class ElasticArrayWriter extends Writer {
    public static class Job extends ElasticWriter.Job {
        
    }

    public static class Task extends ElasticWriter.Task {
        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);
        private int ID_FIELD=0;
        protected boolean isCanRead=true;//当read()返回null时，再次read()将阻塞进程。
        
        @Override
        public void startWrite(RecordReceiver recordReceiver) {
        	if(super.parseArray){
        		RestClient client = this.getClient(this.hostList.get(0));
        		
        		this.isCanRead=true;
        		startWriteWithConn(recordReceiver,client);
        	}
        	else{
        		super.startWrite(recordReceiver);
        	}
        }
        
        protected void startWriteWithConn(RecordReceiver recordReceiver,RestClient conn){
        	List<Record> writeBuffer = new ArrayList<Record>(this.batchSize+30);//合并末尾的同一分组
    		int bufferBytes = 0;
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    writeBuffer.add(record);
                    bufferBytes += record.getMemorySize();

                    if (bufferBytes >= batchByteSize || writeBuffer.size() >= batchSize){
                    	record = trySplitGroupById(recordReceiver,writeBuffer);
                    	if(writeBuffer.size()>2*batchSize)
                    		LOG.warn("====encounter large orders: [{}] docs in a bulk===",writeBuffer.size());
                    	
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
                		EsErrorCode.ERROR, e);
            } finally {
                writeBuffer.clear();
                bufferBytes = 0;
                try {
					conn.close();
				} catch (IOException e) {
				}
                conn = null;
            }
        }
        
        protected void doBulkInsert(RestClient conn,String idx,List<Record> records) throws InterruptedException,IOException{
        	
        	super.postRequest(conn,idx,records);
        }
        protected StringBuilder recordToDoc(String idx,List<Record> records){
        	StringBuilder sb = new StringBuilder();
        	
        	Record[] array = records.toArray(new Record[0]);
        	Map<String,List<Record>> groups = splitArrayToGroup(array);
        	for(Record r : array)//不存在分组,直接构建为doc
        	{
        		if(r!=null)
        			super.appendBulk(sb,idx,r,getNestedDoc(r));
        	}
        		
        	appendArrayProp(sb,idx,groups);//每个分组合并到一个doc
        	return sb;
        }
        
        /**
         * 判断是否仍然后后续”同一分组“数据在reader中， 并继续追加如buffer
         * @return 不属于同组的记录，但已经被取出
         */
        protected Record trySplitGroupById(RecordReceiver recordReceiver,List<Record> writeBuffer){
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
        private void appendArrayProp(StringBuilder sb,String idx,Map<String,List<Record>> groups)
        {
        	for(Entry<String, List<Record>> group : groups.entrySet())
        	{
        		Record r = group.getValue().get(0);
        		Map<String,Object> root = getNestedDoc(r);
        		appendArrayPropsOnly(root,group.getValue());
        		
        		super.appendBulk(sb,idx,r,root);
        	}
        }
        
        private Map<String,Object> getNestedDoc(Record r){
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
        private void appendArrayPropsOnly(Map<String,Object> root, List<Record> records){
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
        private void appendNestedArrayProp(Map<String,Object> root,String props,Column val){
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
        private Map<String,List<Record>> splitArrayToGroup(Record[] records){
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
	        	if(!groups.isEmpty()){
	        		groups.forEach((id,rec)->{
	        			if(rec.size()>3){
	        				LOG.debug("record [{}] merged '{}' nested children",id,rec.size());
	        			}else if(LOG.isTraceEnabled()){
	        				LOG.debug("record [{}] merged '{}' nested children",id,rec.size());
	        			}
	        		});
	        		
	        	}
        	}
        	return groups;
        }
    }
}
