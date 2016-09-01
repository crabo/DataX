package com.alibaba.datax.plugin.writer.verticawriter;

import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.datax.plugin.rdbms.writer.util.OriginalConfPretreatmentUtil;
import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;


public class DeltaCommonRdbmsWriter{

    public static class Job extends CommonRdbmsWriter.Job {
         private static final Logger LOG = LoggerFactory
                 .getLogger(Job.class);

         public Job(DataBaseType dataBaseType) {
        	 super(dataBaseType);
         }
         
         @Override
         public void init(Configuration originalConfig) {
        	 super.init(originalConfig);
        	 
        	 String writeDataSqlTemplate=originalConfig.getString(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK);
        	 if(writeDataSqlTemplate.indexOf("replace INTO")>-1){
        		 writeDataSqlTemplate = StringUtils.replace(writeDataSqlTemplate, "replace INTO", "insert INTO");
        		 originalConfig.set(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK, 
        				 writeDataSqlTemplate);
        	 }
         }
    }

    public static class Task extends CommonRdbmsWriter.Task {
        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);

        public Task(DataBaseType dataBaseType) {
       	 	super(dataBaseType);
        }
        
        String constraintsSql,existsSql,updateSql;
        String getConstraintsSql(){
        	if(constraintsSql==null)
        		constraintsSql="select ANALYZE_CONSTRAINTS('"+this.table+"')";
        	return constraintsSql;
        }
        String getExistsSql(){
        	if(existsSql==null)
        		existsSql= new StringBuilder()
    	            .append("SELECT '1' from ").append(this.table)
    	            .append(" WHERE ").append(getPK()).append("=?")
                .toString();
        	return existsSql;
        }
        String getUpdateSql(){
        	if(updateSql==null)
        		updateSql = new StringBuilder()
	            .append("UPDATE ").append(this.table).append(" SET ")
	            .append(StringUtils.join(this.columns, "=?,")).append("=?")
	            .append(" WHERE ").append(getPK()).append("=?")
	            .toString();
        	return updateSql;
        }
        
        String getPK(){
        	return this.columns.get(0);
        }
        PreparedStatement pareparePK(int pos,PreparedStatement preparedStatement,Record record)throws SQLException{
        	int i=0;
        	int columnSqltype = this.resultSetMetaData.getMiddle().get(i);
            preparedStatement = fillPreparedStatementColumnType(preparedStatement, pos, columnSqltype, record.getColumn(i));
            return preparedStatement;
        }
        
        /**
         * 先执行select id=?操作，存在记录则执行update
         */
        void doUpsertWithCheck(Connection connection,List<Record> buffer){
        	 if(buffer.isEmpty()) return;
        	 
        	 PreparedStatement insertStmt = null,selectStmt=null,updateStmt=null;
             try {
                 connection.setAutoCommit(true);
                 selectStmt = connection
                         .prepareStatement(this.getExistsSql());
                 updateStmt = connection
                         .prepareStatement(this.getUpdateSql());
                 insertStmt = connection
                         .prepareStatement(this.writeRecordSql);
                 int updateCount=0;
                 for (Record record : buffer) {
                     try {
                    	 selectStmt=pareparePK(0,selectStmt,record);
                    	 ResultSet rs = selectStmt.executeQuery();
                    	 if(rs.next()){//&& "1".equals(rs.getString(0))){
                    		 
                    		updateStmt = fillPreparedStatement(
                    				 updateStmt, record);
                    		pareparePK(this.columnNumber,updateStmt,record);//最后一个PK条件
                    	 	updateStmt.execute();
                    	 	updateCount++;
                    	 }else{
                    		 insertStmt = fillPreparedStatement(
                    				 insertStmt, record);
                    		 insertStmt.execute();
                    	 }
                         
                     }catch (SQLException e) {//一条记录异常， 继续进行
                         LOG.debug(e.toString());

                         this.taskPluginCollector.collectDirtyRecord(record, e);
                     } 
                 	 finally {
                         // 重置 preparedStatement，继续进行下一record执行
                    	 selectStmt.clearParameters();
                    	 updateStmt.clearParameters();
                    	 insertStmt.clearParameters();
                     }
                 }
                 
                 if(updateCount>1)
                	 LOG.debug("doOneInsert() update '{}' records.",updateCount);
             } catch (Exception e) {
                 throw DataXException.asDataXException(
                         DBUtilErrorCode.WRITE_DATA_ERROR, e);
             } finally {
                 DBUtil.closeDBResources(selectStmt, null);
                 DBUtil.closeDBResources(updateStmt, null);
                 DBUtil.closeDBResources(insertStmt, null);
             }
        }
        
        /**
         * 当前table约束失效记录存在？
         */
        boolean failAnalyzeConstraints(Connection connection) throws SQLException{
        	Statement stmt=null;
        	try
        	{
	        	stmt= connection.createStatement();
	        	ResultSet rs = stmt.executeQuery(this.getConstraintsSql());
	        	return rs.next();
        	}finally{
        		stmt.close();
        	}
        }
        
        @Override
        protected void doBatchInsert(Connection connection, List<Record> buffer)
                throws SQLException {
        	if("insert".equals(super.writeMode)){//insert模式不检测键冲突,update则检测
        		super.doBatchInsert(connection, buffer);
        		return ;
        	}
        	
            PreparedStatement preparedStatement = null;
            try {
                connection.setAutoCommit(false);
                preparedStatement = connection
                        .prepareStatement(this.writeRecordSql);

                for (Record record : buffer) {
                    preparedStatement = fillPreparedStatement(
                            preparedStatement, record);
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                if(failAnalyzeConstraints(connection))
                {
                	LOG.debug("<violate constraints> rollback '{}' records, fallback to doOneInsert() instead.",buffer.size());
                	connection.rollback();
                	doOneInsert(connection, buffer);
                }else
                	connection.commit();
            } catch (SQLException e) {
                LOG.warn("回滚此次写入, 采用每次写入一行方式提交. 因为:" + e.getMessage());
                connection.rollback();
                doOneInsert(connection, buffer);
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                DBUtil.closeDBResources(preparedStatement, null);
            }
        }
        
        @Override
        protected void doOneInsert(Connection connection, List<Record> buffer) {
        	doUpsertWithCheck(connection,buffer);
        }
        
        @Override
        public void post(Configuration writerSliceConfig) {
        	if("insert".equals(super.writeMode)){//copy模式不检测键冲突
        		Connection connection = DBUtil.getConnection(this.dataBaseType,
                        this.jdbcUrl, username, password);
        		try {
					if(this.failAnalyzeConstraints(connection))
						LOG.warn(">>>>>>>>>> violate constraints！ please run [{}] immediately！<<<<<<<<<<<"
								,getConstraintsSql());
				} catch (SQLException e) {
					e.printStackTrace();
				}finally{
					DBUtil.closeDBResources(null, null, connection);
				}
        	}
        	super.post(writerSliceConfig);
        }
        
        public int getBatchSize()
        {
        	return this.batchSize;
        }
        public int getBatchByteSize()
        {
        	return this.batchByteSize;
        }
        public String getTable()
        {
        	return this.table;
        }
        public String getWriteMode()
        {
        	return this.writeMode;
        }
        public List<String> getColumns()
        {
        	return this.columns;
        }
        public Connection createConnection()
        {
        	return DBUtil.getConnection(this.dataBaseType,
                    this.jdbcUrl, username, password);
        }
    }
}
