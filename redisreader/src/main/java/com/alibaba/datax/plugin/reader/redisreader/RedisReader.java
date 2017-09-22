package com.alibaba.datax.plugin.reader.redisreader;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Created by crabo yang on 17-8-31.
 */
public class RedisReader extends Reader {
	public static class Job extends Reader.Job {
		private static final Logger LOG = LoggerFactory.getLogger(Job.class);

		private Configuration originConfig = null;

		private String host = null;
		private String auth = null;
		private int port = 6897;
		private boolean testOnBorrow;

		private List<String> listKeys = null;
		
		private int connectionTimeout = -1;
		public static JedisPool JEDIS_POOL;

		@Override
		public void init() {
			this.originConfig = this.getPluginJobConf();
			
			host = this.originConfig.getString(Key.HOST);
			auth = this.originConfig.getString(Key.AUTH_PWD);
			port = this.originConfig.getInt(Key.PORT);
			testOnBorrow = this.originConfig.getBool(Key.TEST_ON_BORROW,false);
			
			listKeys = this.originConfig.getList(Key.ListKey, String.class);
			
			connectionTimeout = this.originConfig.getInt(Key.CONN_TIMEOUT);
			
		}

		@Override
		public void prepare() {
			LOG.debug("prepare() begin...");
			if(listKeys.isEmpty())
				throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR
		        		,"No target Keys defined in config.");
			// 生成连接池配置信息
			JedisPoolConfig config = new JedisPoolConfig();
			config.setMaxIdle(10);
			config.setMaxTotal(30);
			config.setMaxWaitMillis(5*1000);
			if(testOnBorrow)
				config.setTestOnBorrow(true);  

			// 在应用初始化的时候生成连接池
			JEDIS_POOL = new JedisPool(config, host, port,connectionTimeout);
			LOG.info(String.format("您即将读取的主题数为: [%s]", this.listKeys.size()));
		}

		@Override
		public void post() {
		}

		@Override
		public void destroy() {
			if(JEDIS_POOL!=null && !JEDIS_POOL.isClosed())
			{
				JEDIS_POOL.close();
				JEDIS_POOL.destroy();
				JEDIS_POOL=null;
			}
		}
		
		

		// warn: 如果源目录为空会报错，拖空目录意图=>空文件显示指定此意图
		@Override
		public List<Configuration> split(int adviceNumber) {
			LOG.debug("split() begin...");
			List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();

			// warn:每个slice拖且仅拖一个文件,
			// int splitNumber = adviceNumber;
			int splitNumber = this.listKeys.size();
            if (0 == splitNumber) {
                throw DataXException.asDataXException(
                		CommonErrorCode.CONFIG_ERROR, String
                                .format("未能找到待读取的主题,请确认您的配置项listKey: %s",
                                        this.originConfig.getString(Key.ListKey)));
            }

			List<List<String>> splitedSourceFiles = this.splitSourceFiles(
					this.listKeys, splitNumber);
			
			for (List<String> topics : splitedSourceFiles) {
				Configuration splitedConfig = this.originConfig.clone();
				splitedConfig.set(Constant.SOURCE_KEYS, topics);
				splitedConfig.set(Key.AUTH_PWD, auth);
				
				readerSplitConfigs.add(splitedConfig);
			}
			LOG.debug("split() ok and end...");
			return readerSplitConfigs;
		}
		

		private <T> List<List<T>> splitSourceFiles(final List<T> sourceList,
				int adviceNumber) {
			List<List<T>> splitedList = new ArrayList<List<T>>();
			int averageLength = sourceList.size() / adviceNumber;
			averageLength = averageLength == 0 ? 1 : averageLength;

			for (int begin = 0, end = 0; begin < sourceList.size(); begin = end) {
				end = begin + averageLength;
				if (end > sourceList.size()) {
					end = sourceList.size();
				}
				splitedList.add(sourceList.subList(begin, end));
			}
			return splitedList;
		}

	}

	public static class Task extends Reader.Task {
		private static Logger LOG = LoggerFactory.getLogger(Task.class);

		private Configuration readerSliceConfig;
		private List<String> sourceKeys;
		private int readBatchSize;

		@Override
		public void init() {
			this.readerSliceConfig = this.getPluginJobConf();
			this.sourceKeys = this.readerSliceConfig.getList(
					Constant.SOURCE_KEYS, String.class);
			
			this.readBatchSize = this.readerSliceConfig.getInt(
					Key.READ_BATCH_SIZE, 100);
		}

		@Override
		public void prepare() {
		}

		@Override
		public void post() {

		}

		@Override
		public void destroy() {
		}
		
		private Jedis getRedisClient() {
			Jedis redisClient=null;
		      try {
		    	  redisClient = Job.JEDIS_POOL.getResource();
		    	  
		    	  if(readerSliceConfig.getString(Key.AUTH_PWD)!=null)
		    		  redisClient.auth(readerSliceConfig.getString(Key.AUTH_PWD));
		    	  
		      } catch (Exception e) {
			        LOG.error("Can't create redis from pool", e);
			        throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR
			        		,"Can't create redis from pool");
		      }
		    return redisClient;
		  }

		@Override
		public void startRead(RecordSender recordSender) {
			LOG.debug("start read source files...");
			
			List<ColumnEntry> columns = JsonStorageReaderUtil.getListColumnEntry(readerSliceConfig, Key.COLUMN);
			TaskPluginCollector collector = super.getTaskPluginCollector();
			Jedis client = getRedisClient();
			for (String cacheKey : this.sourceKeys) {
				LOG.debug(String.format("reading redis cache : [%s]", cacheKey));
				List<String> json=null;
				int i=0;
				try {
					do{
						int batch_start_pos = readBatchSize*i;
						//读取头部批量数据
						json = client.lrange(cacheKey, batch_start_pos, batch_start_pos+readBatchSize-1);
						for(String row : json){
							JsonStorageReaderUtil.transportOneRecord(recordSender, collector, 
									columns, row);
						}
						
						i++;
						if(i>5){//每一批次flush成功，则从redis删除本批次
							recordSender.flush();
							//仅保留从头部起 [batch*i, END] 之间的数据
							client.ltrim(cacheKey, batch_start_pos, -1);
							i=0;
						}
					}while(!json.isEmpty());
					
					if(i>0){
						recordSender.flush();
						client.ltrim(cacheKey, readBatchSize*i, -1);
					}
				} catch (Exception e) {
					String message = String
							.format("error reading redis : [%s]", cacheKey);
					LOG.error(message,e);
					
					throw DataXException.asDataXException(
							CommonErrorCode.RUNTIME_ERROR, message);
				}finally{
					client.close();
				}
			}
			LOG.debug("end read redis ...");
		}

	}
}
