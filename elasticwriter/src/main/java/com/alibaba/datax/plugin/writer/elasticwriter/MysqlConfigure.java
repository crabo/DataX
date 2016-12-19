package com.alibaba.datax.plugin.writer.elasticwriter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.plugin.writer.elasticwriter.ElasticBatchWriter.Job;


public class MysqlConfigure {
	Properties config;
	public MysqlConfigure(){
		config = new Properties();
		try {
			InputStream in = new FileInputStream(System.getProperty("user.dir")+"/conf/mysql.properties");
			config.load(in);
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

	public List<Map<String,String>> getConfig(){
		String sql = config.getProperty("jdbc.querySql");
		List<Map<String,String>> li = execute(sql);
		
		return li;
	}
	
	public List<Map<String,String>> execute(String sql){
		List<Map<String,String>> li = new ArrayList<>();
		
	    try (Connection conn = connect()) {
	        try (Statement stmt = conn.createStatement()) {
	          ResultSet rs = stmt.executeQuery(sql);
	          
	          ResultSetMetaData metaData = rs.getMetaData();
	          int columnNumber = metaData.getColumnCount();
	          
	          while(rs.next()){
		          Map<String,String> record=new HashMap<>(columnNumber);
		          for(int i=1;i<=columnNumber;i++){
		        	  record.put(metaData.getColumnLabel(i), rs.getString(i));
		          }
		          li.add(record);
	          }
	          
	          stmt.close();
	      }
	      conn.close();
	    }
	    catch(Exception ex){
	    	throw new RuntimeException(sql,ex);
	    }
	    return li;
	  }
	
	
	protected Connection connect() throws Exception{
		Class.forName("com.mysql.jdbc.Driver");
		return DriverManager.getConnection(
				config.getProperty("jdbc.url"), 
				config.getProperty("jdbc.user"), 
				config.getProperty("jdbc.pwd"));
	    /*HikariConfig hikariConfig = new HikariConfig();
	    hikariConfig.setJdbcUrl(config.getProperty("jdbc.url"));
	    hikariConfig.setUsername(config.getProperty("jdbc.user"));
	    hikariConfig.setPassword(config.getProperty("jdbc.pwd"));
	    hikariConfig.addDataSourceProperty("useSSL", false);
	    hikariConfig.setAutoCommit(false);
	    return new HikariDataSource(hikariConfig);*/
	}
}
