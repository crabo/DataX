package com.alibaba.datax.plugin.writer.elasticwriter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class MysqlConfigure {
	static SimpleDateFormat TS_FORMAT=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
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
		List<Map<String,String>> li = executeQuery(sql);
		
		return li;
	}
	
	public void updateProgress(String database,Date time){
		String sql = config.getProperty("jdbc.updateSql")
						.replace("$ts_key", database)
						.replace("$ts_value", TS_FORMAT.format(time));
		executeUpdate(sql);
	}
	
	public void updateError(String database,Exception error){
		String msg=error.getMessage();
		if(msg.length()>240)
			msg = msg.substring(0, 240);
		
		String sql = config.getProperty("jdbc.errorSql")
						.replace("$ts_key", database)
						.replace("$ts_error", (new Date().toString())+msg);
		executeUpdate(sql);
	}
	
	private List<Map<String,String>> executeQuery(String sql){
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
	
	private int executeUpdate(String sql){
		int count=0;
	    try (Connection conn = connect()) {
	        try (Statement stmt = conn.createStatement()) {
	          count = stmt.executeUpdate(sql);
	          stmt.close();
	      }
	      conn.close();
	    }
	    catch(Exception ex){
	    	throw new RuntimeException(sql,ex);
	    }
	    return count;
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
