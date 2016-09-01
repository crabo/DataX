package com.alibaba.datax.plugin.writer.verticawriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.datax.plugin.rdbms.writer.util.OriginalConfPretreatmentUtil;
import com.vertica.jdbc.VerticaConnection;
import com.vertica.jdbc.VerticaCopyStream;

/**
 * writeMode:
 * replace=COPY FROM 的csv模式
 * insert = batchInsert,全部完成后执行 constraints 检查。
 * update = 逐个batch执行， 立即constraints检查， 出错后采用oneInsert逐条插入或更新。
 */
public class VerticaWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.Vertica;

    public static class Job extends Writer.Job {
        private Configuration originalConfig = null;
        private DeltaCommonRdbmsWriter.Job commonRdbmsWriterJob;

        @Override
        public void preCheck(){
            this.init();
            this.commonRdbmsWriterJob.writerPreCheck(this.originalConfig, DATABASE_TYPE);
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.commonRdbmsWriterJob = new DeltaCommonRdbmsWriter.Job(DATABASE_TYPE);
            this.commonRdbmsWriterJob.init(this.originalConfig);
        }

        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
        @Override
        public void prepare() {
            //实跑先不支持 权限 检验
            //this.commonRdbmsWriterJob.privilegeValid(this.originalConfig, DATABASE_TYPE);
            this.commonRdbmsWriterJob.prepare(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return this.commonRdbmsWriterJob.split(this.originalConfig, mandatoryNumber);
        }

        // 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
        @Override
        public void post() {
            this.commonRdbmsWriterJob.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterJob.destroy(this.originalConfig);
        }

    }

    public static class Task extends Writer.Task {
    	private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);
        private Configuration writerSliceConfig;
        private DeltaCommonRdbmsWriter.Task commonRdbmsWriterTask;
        private String copyFromEncoding;
        private int batchSize;
        private int batchByteSize;
        private int columnNumber;
        private String fieldDelimiter;
        private String tempFile;
        private static final String NEWLINE_FLAG = System.getProperty("line.separator", "\n");

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            //by crabo: 未生成insert 语句？
            if(null == writerSliceConfig.getString(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK))
            	OriginalConfPretreatmentUtil.doPretreatment(this.writerSliceConfig,DATABASE_TYPE);
            this.commonRdbmsWriterTask = new DeltaCommonRdbmsWriter.Task(DATABASE_TYPE);
            this.commonRdbmsWriterTask.init(this.writerSliceConfig);
            
            this.copyFromEncoding = writerSliceConfig.getString("copyFromEncoding","delimiter E'\\t' NULL 'NULL'");
            this.fieldDelimiter = this.writerSliceConfig.getString("fieldDelimiter", "\t");
            this.tempFile = "vertica_job_"+new Date().getTime()+".csv";
        }

        @Override
        public void prepare() {
            this.commonRdbmsWriterTask.prepare(this.writerSliceConfig);
        }

        //TODO 改用连接池，确保每次获取的连接都是可用的（注意：连接可能需要每次都初始化其 session）
        public void startWrite(RecordReceiver recordReceiver) {
        	if("replace".equals(commonRdbmsWriterTask.getWriteMode()))
        		parepareCopyStream(recordReceiver);
        	else
        		this.commonRdbmsWriterTask.startWrite(recordReceiver, this.writerSliceConfig,
                    super.getTaskPluginCollector());
        }

        @Override
        public void post() {
        	File temp = new File(this.tempFile);
        	if(temp.exists())
        		temp.delete();
        }

        @Override
        public void destroy() {
            
        }

        @Override
        public boolean supportFailOver(){
            String writeMode = writerSliceConfig.getString(Key.WRITE_MODE);
            return "replace".equalsIgnoreCase(writeMode);
        }
        String copyfrom;
        String getCopyFrom(){
        	if(copyfrom==null){
        		copyfrom=String.format("copy %s (%s) from STDIN %s REJECTED DATA E'%s.log' DIRECT no commit",
            			commonRdbmsWriterTask.getTable(), 
            			StringUtils.join(commonRdbmsWriterTask.getColumns(),','), 
            			this.copyFromEncoding,
            			this.tempFile);
        	}
        	return copyfrom;
        }
        
        void parepareCopyStream(RecordReceiver recordReceiver){
        	this.batchSize=commonRdbmsWriterTask.getBatchSize();
        	this.batchByteSize = commonRdbmsWriterTask.getBatchByteSize();
        	this.columnNumber = commonRdbmsWriterTask.getColumns().size();
        	
        	try {
        		Connection connection = commonRdbmsWriterTask.createConnection();
				connection.setAutoCommit(false);
				
				LOG.info("start COPY FROM with: {}",getCopyFrom());
				startWriteWithConnection(recordReceiver,connection);
			} catch (SQLException e) {
				throw DataXException.asDataXException(
                        DBUtilErrorCode.SET_SESSION_ERROR, e);
			}
        }
        
        public void startWriteWithConnection(RecordReceiver recordReceiver,Connection conn){
        	FileOutputStream fs = null;
        	
        	int bufferBytes = 0;
            int count=0;
            try {
            	VerticaCopyStream vertica = new VerticaCopyStream((VerticaConnection)conn,getCopyFrom());
            	
            	fs = new FileOutputStream(this.tempFile);
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                	fs.write(recordToBytes(record));
                    bufferBytes += record.getMemorySize();
                    count++;

                    if (count >= batchSize || bufferBytes>=this.batchByteSize) {
                    	fs.close();
                    	fs = flushBatch(fs,vertica);
                    	conn.commit();
                    	
                        bufferBytes = 0;
                        count=0;
                    }
                }
                
                fs = flushBatch(fs,vertica);
                conn.commit();
                
                bufferBytes=0;
            } catch (Exception e) {
            	try {
					conn.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
            	
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                bufferBytes = 0;
                DBUtil.closeDBResources(null, null, conn);
                
                try {
                	if(fs!=null)
                		fs.close();
                	
				} catch (IOException e) {
					e.printStackTrace();
				}
            }
        }
        FileOutputStream flushBatch(FileOutputStream writer,VerticaCopyStream vertica) throws IOException, SQLException{
        	writer.close();
        	
        	FileInputStream reader = new FileInputStream(this.tempFile);
        	vertica.addStream(reader);
        	vertica.execute();
        	long count = vertica.finish();
        	LOG.debug("flush COPY FROM with '{}' records",count);
        	reader.close();
        	
        	writer=new FileOutputStream(this.tempFile);
        	return writer;
        }
        
        private byte[] recordToBytes(Record record) {
        	return recordToString(record).getBytes(Charset.forName("utf-8"));
        }
        private String recordToString(Record record) {
            int recordLength = record.getColumnNumber();
            if (0 == recordLength) {
                return NEWLINE_FLAG;
            }

            Column column;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < recordLength; i++) {
                column = record.getColumn(i);
                sb.append(column.asString()).append(fieldDelimiter);
            }
            sb.setLength(sb.length() - 1);
            sb.append(NEWLINE_FLAG);

            return sb.toString();
        }
        
    }//Task
}
