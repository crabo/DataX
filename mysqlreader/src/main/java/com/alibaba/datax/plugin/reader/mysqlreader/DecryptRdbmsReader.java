package com.alibaba.datax.plugin.reader.mysqlreader;


import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.reader.mysqlreader.MysqlReader.Job;
import com.taobao.api.SecretException;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * 如果reader参数存在decryptKey时，调用taobao SDK直接解密
 * 替换record中指定index位置的一批字段值为解密后的结果
 * @author crabo
 *
 */
public class DecryptRdbmsReader {
	
	public static class Task extends CommonRdbmsReader.Task{
		private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);
		
		private final TaobaoSecurityClient securityClient;
		private ArrayList<Integer> decryptColumns;
		private ArrayList<String> decryptColumnTypes;
		private String shopId;
		
		/**
		 * 
		 * @param dataBaseType
		 * @param taskGropuId
		 * @param taskId
		 * @param decryptKey
		 * @param todecryptColumns   phone:1,nick:3
		 */
		public Task(DataBaseType dataBaseType, int taskGropuId, int taskId
				,String decryptUrl,String todecryptColumns) {
			super(dataBaseType, taskGropuId, taskId);
			
			if(StringUtils.isNotBlank(todecryptColumns))
			{

				decryptUrl = decryptUrl.replace("\r","").replace("\n","");
				this.shopId = decryptUrl.substring(decryptUrl.indexOf("=")+1,decryptUrl.indexOf("&")).trim();
				securityClient =  new TaobaoSecurityClient(decryptUrl);

				decryptColumns = new ArrayList<Integer>();
				decryptColumnTypes= new ArrayList<String>();
				for(String col : StringUtils.split(todecryptColumns,","))
				{
					String[] typeIdx = StringUtils.split(col,":");
					decryptColumnTypes.add(typeIdx[0]);
					decryptColumns.add(Integer.valueOf(typeIdx[1]));
				}
				LOG.info("prepare to decrypt columns:"+todecryptColumns);
			}else
				securityClient=null;
		}
		
		@Override
		protected Record transportOneRecord(RecordSender recordSender, ResultSet rs, 
                ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding, 
                TaskPluginCollector taskPluginCollector) {
            try{
            Record record = buildRecord(recordSender,rs,metaData,columnNumber,mandatoryEncoding,taskPluginCollector); 
            
            boolean hasEncrypt=false;
            //将每一列解密后， 原位替换
            for(int i=0;i<decryptColumns.size();i++){
            	
            	int col = decryptColumns.get(i);
            	
            	String encryptText =record.getColumn(col).asString();
            	if(!StringUtils.isBlank(encryptText))
            	{
	            	String plainText = decryptText( 
	            			encryptText
	            			,decryptColumnTypes.get(i)
	            		);
	            	record.setColumn(col, new StringColumn(plainText));
	            	
	            	if(!hasEncrypt) hasEncrypt=encryptText.indexOf("~")>-1;
	            	if(count<50 && hasEncrypt)
	            	{
	            		LOG.info("decrypt column#{} value '{}' to '{}'",col
	            				,encryptText
	            				,plainText);
	            		count++;
	            	}
            	}
            }
            
            recordSender.sendToWriter(record);
            return record;
            }catch(Exception e){
                LOG.error("transport record error in shop '{}':{}",this.shopId,e);
                throw e;
            }
            
        }
		private int count=0;
		
		private String decryptText(String encyptString,String type){
			try {
				return securityClient.decrypt(encyptString, type,this.shopId);
			} catch (SecretException e) {
				LOG.warn("decryptText text '{}' with '{}' error! \n{}",encyptString,type,e);
				throw DataXException.asDataXException(
                        DBUtilErrorCode.UNSUPPORTED_TYPE,"decryptText('"+encyptString+"') error occured!",e);
			}
		}
		
		
	}
}
