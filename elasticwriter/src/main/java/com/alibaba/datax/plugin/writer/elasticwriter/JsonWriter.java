
package com.alibaba.datax.plugin.writer.elasticwriter;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
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
        
        private Configuration writerSliceConfig;
        protected int batchSize;
        protected int batchByteSize;
        protected String writeMode;
        protected boolean parseArray;
        protected List<String> columns;
        protected int columnNumber;



        @Override
        public void init() {
            this.writerSliceConfig = getPluginJobConf();
            
            this.batchSize = writerSliceConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
            this.batchByteSize = writerSliceConfig.getInt(Key.BATCH_BYTE_SIZE, Constant.DEFAULT_BATCH_BYTE_SIZE);
            this.writeMode = writerSliceConfig.getString(Key.WRITE_MODE, "update");//index 或  update
            this.columns = writerSliceConfig.getList(Key.COLUMN, String.class);
            this.parseArray= writerSliceConfig.getBool("parseArray", false);
            this.columnNumber = this.columns.size();
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
        	BufferedOutputStream fs = null;
			try {
				fs = new BufferedOutputStream(
						new FileOutputStream("es_data.json")
	        	        //new GZIPOutputStream(new FileOutputStream("es_data.json.gz"))
	        	        );
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
        
        final static Charset UTF_8=Charset.forName("utf-8");
        private void doBulkInsert(BufferedOutputStream conn,List<Record> records) throws IOException{
        	for(Record r : records)
        	{
        		conn.write(JSON.toJSONString(
        				this.getNestedDoc(r)).getBytes(UTF_8));
        		conn.write("\n".getBytes(UTF_8));
        	}
        	conn.flush();
        }
        
        private Map<String,Object> getNestedDoc(Record r){
        	Map<String,Object> root = new HashMap<String,Object>();
        	for(int i=0;i<this.columnNumber;i++){
        		String colName = this.columns.get(i);
    			appendNestedProp(root,colName,r.getColumn(i));
        	}
        	return root;
        }
        

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }
}
