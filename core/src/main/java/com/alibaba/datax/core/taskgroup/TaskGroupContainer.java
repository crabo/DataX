package com.alibaba.datax.core.taskgroup;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.AbstractContainer;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.container.communicator.taskgroup.StandaloneTGContainerCommunicator;
import com.alibaba.datax.core.statistics.plugin.task.AbstractTaskPluginCollector;
import com.alibaba.datax.core.taskgroup.runner.AbstractRunner;
import com.alibaba.datax.core.taskgroup.runner.ReaderRunner;
import com.alibaba.datax.core.taskgroup.runner.WriterRunner;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.exchanger.BufferedRecordExchanger;
import com.alibaba.datax.core.transport.exchanger.BufferedRecordTransformerExchanger;
import com.alibaba.datax.core.transport.transformer.TransformerExecution;
import com.alibaba.datax.core.util.ClassUtil;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.TransformerUtil;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;

public class TaskGroupContainer extends AbstractContainer {
    private static final Logger LOG = LoggerFactory
            .getLogger(TaskGroupContainer.class);

    /**
     * 当前taskGroup所属jobId
     */
    private long jobId;

    /**
     * 当前taskGroupId
     */
    private int taskGroupId;

    /**
     * 使用的channel类
     */
    private String channelClazz;

    /**
     * task收集器使用的类
     */
    private String taskCollectorClass;

    private TaskMonitor taskMonitor = TaskMonitor.getInstance();

    public TaskGroupContainer(Configuration configuration) {
        super(configuration);

        initCommunicator(configuration);

        this.jobId = this.configuration.getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
        this.taskGroupId = this.configuration.getInt(
                CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);

        this.channelClazz = this.configuration.getString(
                CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CLASS);
        this.taskCollectorClass = this.configuration.getString(
                CoreConstant.DATAX_CORE_STATISTICS_COLLECTOR_PLUGIN_TASKCLASS);
    }

    private void initCommunicator(Configuration configuration) {
        super.setContainerCommunicator(new StandaloneTGContainerCommunicator(configuration));

    }

    public long getJobId() {
        return jobId;
    }

    public int getTaskGroupId() {
        return taskGroupId;
    }

    @Override
    public void start() {
        try {
            /**
             * 状态check时间间隔，较短，可以把任务及时分发到对应channel中
             */
            int sleepIntervalInMillSec = this.configuration.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_SLEEPINTERVAL, 100);
            /**
             * 状态汇报时间间隔，稍长，避免大量汇报
             */
            long reportIntervalInMillSec = this.configuration.getLong(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_REPORTINTERVAL,
                    10000);
            /**
             * 2分钟汇报一次性能统计
             */

            // 获取channel数目
            int channelNumber = this.configuration.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL);

            int taskMaxRetryTimes = this.configuration.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASK_FAILOVER_MAXRETRYTIMES, 1);

            long taskRetryIntervalInMsec = this.configuration.getLong(
                    CoreConstant.DATAX_CORE_CONTAINER_TASK_FAILOVER_RETRYINTERVALINMSEC, 10000);

            long taskMaxWaitInMsec = this.configuration.getLong(CoreConstant.DATAX_CORE_CONTAINER_TASK_FAILOVER_MAXWAITINMSEC, 60000);
            
            List<Configuration> taskConfigs = this.configuration
                    .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
            if(LOG.isDebugEnabled()) {
                LOG.debug("taskGroup[{}]'s task configs[{}]", this.taskGroupId,
                        JSON.toJSONString(taskConfigs));
            }
            
            int taskCountInThisTaskGroup = taskConfigs.size();
            LOG.debug(String.format(
                    "taskGroupId=[%d] start [%d] channels for [%d] tasks.",
                    this.taskGroupId, channelNumber, taskCountInThisTaskGroup));
            
            this.containerCommunicator.registerCommunication(taskConfigs);
            int ts_interval = this.configuration.getInt("job.setting.ts_interval_sec",0);
            do{
            
	            Map<Integer, Configuration> taskConfigMap = buildTaskConfigMap(taskConfigs); //taskId与task配置
	            List<Configuration> taskQueue = buildRemainTasks(taskConfigs); //待运行task列表
	            Map<Integer, TaskExecutor> taskFailedExecutorMap = new HashMap<Integer, TaskExecutor>(); //taskId与上次失败实例
	            List<TaskExecutor> runTasks = new ArrayList<TaskExecutor>(channelNumber); //正在运行task
	            Map<Integer, Long> taskStartTimeMap = new HashMap<Integer, Long>(); //任务开始时间
	
	            long lastReportTimeStamp = 0;
	            Communication lastTaskGroupContainerCommunication = new Communication();
	            
	            DeltaJobTimestamp.read(this.configuration);//by crabo ts_start：从外部文件读取时间戳
	            while (true) {
	            	//1.判断task状态
	            	boolean failedOrKilled = false;
	            	Map<Integer, Communication> communicationMap = containerCommunicator.getCommunicationMap();
	            	for(Map.Entry<Integer, Communication> entry : communicationMap.entrySet()){
	            		Integer taskId = entry.getKey();
	            		Communication taskCommunication = entry.getValue();
	                    if(!taskCommunication.isFinished()){
	                        continue;
	                    }
	                    TaskExecutor taskExecutor = removeTask(runTasks, taskId);
	
	                    //上面从runTasks里移除了，因此对应在monitor里移除
	                    taskMonitor.removeTask(taskId);
	
	                    //失败，看task是否支持failover，重试次数未超过最大限制
	            		if(taskCommunication.getState() == State.FAILED){
	                        taskFailedExecutorMap.put(taskId, taskExecutor);
	            			if(taskExecutor.supportFailOver() && taskExecutor.getAttemptCount() < taskMaxRetryTimes){
	                            taskExecutor.shutdown(); //关闭老的executor
	                            containerCommunicator.resetCommunication(taskId); //将task的状态重置
	            				Configuration taskConfig = taskConfigMap.get(taskId);
	            				taskQueue.add(taskConfig); //重新加入任务列表
	            			}else{
	            				failedOrKilled = true;
	                			break;
	            			}
	            		}else if(taskCommunication.getState() == State.KILLED){
	            			failedOrKilled = true;
	            			break;
	            		}else if(taskCommunication.getState() == State.SUCCEEDED){
	                        Long taskStartTime = taskStartTimeMap.get(taskId);
	                        if(taskStartTime != null){
	                            Long usedTime = System.currentTimeMillis() - taskStartTime;
	                            LOG.info("taskGroup[{}] taskId[{}] is successed, used[{}]s",
	                                    this.taskGroupId, taskId, usedTime/1000);
	                            //usedTime*1000*1000 转换成PerfRecord记录的ns，这里主要是简单登记，进行最长任务的打印。因此增加特定静态方法
	                            PerfRecord.addPerfRecord(taskGroupId, taskId, PerfRecord.PHASE.TASK_TOTAL,taskStartTime, usedTime * 1000L * 1000L);
	                            taskStartTimeMap.remove(taskId);
	                            taskConfigMap.remove(taskId);
	                        }
	                    }
	            	}
	            	
	                // 2.发现该taskGroup下taskExecutor的总状态失败则汇报错误
	                if (failedOrKilled) {
	                    lastTaskGroupContainerCommunication = reportTaskGroupCommunication(
	                            lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);
	                    Throwable ex = lastTaskGroupContainerCommunication.getThrowable();
	                    DeltaJobTimestamp.notifyError(this.configuration, ex);
	                    if(ex instanceof OutOfMemoryError){//by crabo
	                    	LOG.info("taskGroup OutOfMemoryError occurred!!! job exit now.");
	                    	System.exit(9);
	                    }
	                    
	                    throw DataXException.asDataXException(
	                            FrameworkErrorCode.PLUGIN_RUNTIME_ERROR, ex);
	                }
	                
	                //3.有任务未执行，且正在运行的任务数小于最大通道限制
	                Iterator<Configuration> iterator = taskQueue.iterator();
	                while(iterator.hasNext() && runTasks.size() < channelNumber){
	                    Configuration taskConfig = iterator.next();
	                    Integer taskId = taskConfig.getInt(CoreConstant.TASK_ID);
	                    int attemptCount = 1;
	                    TaskExecutor lastExecutor = taskFailedExecutorMap.get(taskId);
	                    if(lastExecutor!=null){
	                        attemptCount = lastExecutor.getAttemptCount() + 1;
	                        long now = System.currentTimeMillis();
	                        long failedTime = lastExecutor.getTimeStamp();
	                        if(now - failedTime < taskRetryIntervalInMsec){  //未到等待时间，继续留在队列
	                            continue;
	                        }
	                        if(!lastExecutor.isShutdown()){ //上次失败的task仍未结束
	                            if(now - failedTime > taskMaxWaitInMsec){
	                                markCommunicationFailed(taskId);
	                                reportTaskGroupCommunication(lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);
	                                throw DataXException.asDataXException(CommonErrorCode.WAIT_TIME_EXCEED, "task failover等待超时");
	                            }else{
	                                lastExecutor.shutdown(); //再次尝试关闭
	                                continue;
	                            }
	                        }else{
	                            LOG.info("taskGroup[{}] taskId[{}] attemptCount[{}] has already shutdown",
	                                    this.taskGroupId, taskId, lastExecutor.getAttemptCount());
	                        }
	                    }
	                    Configuration taskConfigForRun = taskMaxRetryTimes > 1 ? taskConfig.clone() : taskConfig;
	                    
	                    if(ts_interval>0)
	                    	DeltaJobTimestamp.apply(this.configuration,taskConfigForRun);//by crabo ts_start：从外部文件读取时间戳
	                	TaskExecutor taskExecutor = new TaskExecutor(taskConfigForRun, attemptCount);
	                    taskStartTimeMap.put(taskId, System.currentTimeMillis());
	                	taskExecutor.doStart();
	
	                    iterator.remove();
	                    runTasks.add(taskExecutor);
	
	                    //上面，增加task到runTasks列表，因此在monitor里注册。
	                    taskMonitor.registerTask(taskId, this.containerCommunicator.getCommunication(taskId));
	
	                    taskFailedExecutorMap.remove(taskId);
	                    LOG.debug("taskGroup[{}] taskId[{}] attemptCount[{}] is started",
	                            this.taskGroupId, taskId, attemptCount);
	                }
	
	                //4.任务列表为空，executor已结束, 搜集状态为success--->成功
	                /*
	                if (taskQueue.isEmpty() && isAllTaskDone(runTasks) && containerCommunicator.collectState() == State.SUCCEEDED) {
	                	// 成功的情况下，也需要汇报一次。否则在任务结束非常快的情况下，采集的信息将会不准确
	                    lastTaskGroupContainerCommunication = reportTaskGroupCommunication(
	                            lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);
	                    
	                    LOG.debug("taskGroup[{}] completed it's tasks.", this.taskGroupId);
	                    break;
	                }*/
	                if (taskQueue.isEmpty() && isAllTaskDone(runTasks) && containerCommunicator.collectState() == State.SUCCEEDED) {
	                    if(ts_interval>0){
		                    DeltaJobTimestamp.write(this.configuration);//by crabo ts_start:更新时间戳
		                    for(TaskExecutor taskExecutor:runTasks){
		                        taskMonitor.report(taskExecutor.getTaskId(),this.containerCommunicator.getCommunication(taskExecutor.getTaskId()));
		                    }
		                    
		                    for(Communication c : containerCommunicator.getCommunicationMap().values())
		                		c.setState(State.RUNNING,true);
		                    break;//break to wait next 60s run
	                    }
	                    
	                	// 成功的情况下，也需要汇报一次。否则在任务结束非常快的情况下，采集的信息将会不准确
	                	lastTaskGroupContainerCommunication = reportTaskGroupCommunication(
	                            lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);
	                	
	                    LOG.debug("taskGroup[{}] completed it's tasks.", this.taskGroupId);
	                    break;
	                }
	
	                // 5.如果当前时间已经超出汇报时间的interval，那么我们需要马上汇报
	                long now = System.currentTimeMillis();
	                if (now - lastReportTimeStamp > reportIntervalInMillSec) {
	                    lastTaskGroupContainerCommunication = reportTaskGroupCommunication(
	                            lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);
	
	                    lastReportTimeStamp = now;
	
	                    //taskMonitor对于正在运行的task，每reportIntervalInMillSec进行检查
	                    for(TaskExecutor taskExecutor:runTasks){
	                        taskMonitor.report(taskExecutor.getTaskId(),this.containerCommunicator.getCommunication(taskExecutor.getTaskId()));
	                    }
	
	                }
	
	                Thread.sleep(sleepIntervalInMillSec);
	            }//end while true
	        	
            	//6.最后还要汇报一次
            	reportTaskGroupCommunication(lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);
            }while(ts_interval>0);

        } catch (Throwable e) {
            Communication nowTaskGroupContainerCommunication = this.containerCommunicator.collect();

            if (nowTaskGroupContainerCommunication.getThrowable() == null) {
                nowTaskGroupContainerCommunication.setThrowable(e);
            }
            nowTaskGroupContainerCommunication.setState(State.FAILED);
            this.containerCommunicator.report(nowTaskGroupContainerCommunication);
            
            DeltaJobTimestamp.notifyError(this.configuration, e);
            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR, e);
        }finally {
            if(!PerfTrace.getInstance().isJob()){
                //最后打印cpu的平均消耗，GC的统计
                VMInfo vmInfo = VMInfo.getVmInfo();
                if (vmInfo != null) {
                    vmInfo.getDelta(false);
                    LOG.info(vmInfo.totalString());
                }

                LOG.info(PerfTrace.getInstance().summarizeNoException());
            }
        }
    }
    
    private Map<Integer, Configuration> buildTaskConfigMap(List<Configuration> configurations){
    	Map<Integer, Configuration> map = new HashMap<Integer, Configuration>();
    	for(Configuration taskConfig : configurations){
        	int taskId = taskConfig.getInt(CoreConstant.TASK_ID);
        	map.put(taskId, taskConfig);
    	}
    	return map;
    }

    private List<Configuration> buildRemainTasks(List<Configuration> configurations){
    	List<Configuration> remainTasks = new LinkedList<Configuration>();
    	for(Configuration taskConfig : configurations){
    		remainTasks.add(taskConfig);
    	}
    	return remainTasks;
    }
    
    private TaskExecutor removeTask(List<TaskExecutor> taskList, int taskId){
    	Iterator<TaskExecutor> iterator = taskList.iterator();
    	while(iterator.hasNext()){
    		TaskExecutor taskExecutor = iterator.next();
    		if(taskExecutor.getTaskId() == taskId){
    			iterator.remove();
    			return taskExecutor;
    		}
    	}
    	return null;
    }
    
    private boolean isAllTaskDone(List<TaskExecutor> taskList){
    	for(TaskExecutor taskExecutor : taskList){
    		if(!taskExecutor.isTaskFinished()){
    			return false;
    		}
    	}
    	return true;
    }

    private Communication reportTaskGroupCommunication(Communication lastTaskGroupContainerCommunication, int taskCount){
        Communication nowTaskGroupContainerCommunication = this.containerCommunicator.collect();
        nowTaskGroupContainerCommunication.setTimestamp(System.currentTimeMillis());
        Communication reportCommunication = CommunicationTool.getReportCommunication(nowTaskGroupContainerCommunication,
                lastTaskGroupContainerCommunication, taskCount);
        this.containerCommunicator.report(reportCommunication);
        return reportCommunication;
    }

    private void markCommunicationFailed(Integer taskId){
        Communication communication = containerCommunicator.getCommunication(taskId);
        communication.setState(State.FAILED);
    }
    
    static class DeltaJobTimestamp{
    	private static final Logger LOG = LoggerFactory
                .getLogger(DeltaJobTimestamp.class);
    	
    	public static void notifyError(Configuration cfg,Throwable ex){
    		String file = cfg.getString("job.setting.ts_file","job.ts.txt");//timestamp在当前目录下的文件名。 不配置job.setting.ts_interval_sec则不启用deltaJob
    		File f = new File(file+".error");
    		FileWriter fw=null;
			try {
				fw = new FileWriter(f);
				fw.append(getIntervalTime(null,0,0,0));
				fw.append(" - \t");
				fw.append(ex==null?"no exception.":ex.toString());
			} catch (IOException e) {
				e.printStackTrace();
			}finally{
				if(fw!=null)
					try {
						fw.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
			}
    		
    	}
    	
    	public static void read(Configuration cfg){
    		String file = cfg.getString("job.setting.ts_file","job.ts.txt");//timestamp在当前目录下的文件名。 不配置job.setting.ts_interval_sec则不启用deltaJob
    		
    		String start=fromFile(file);
            if(start==null || start.length()==0)//文件读取异常？从上次job中读取
                start = cfg.getString("job.setting.ts_end",getIntervalTime(null,0,cfg.getInt("job.setting.ts_adjustnow_sec",0),0));
            
            cfg.set("job.setting.ts_start", start);
    		cfg.set("job.setting.ts_end", getIntervalTime(start
    					,cfg.getInt("job.setting.ts_batch_mins",0)
    					,cfg.getInt("job.setting.ts_adjustnow_sec",0)
    					,cfg.getInt("job.setting.ts_interval_sec",0)
    				));
    	}
    	public static void apply(Configuration cfg,Configuration task){
    		String sql = task.getString("reader.parameter.ts_querySql");//original sql
    		if(sql==null)
    		{
    			sql=task.getString("reader.parameter.querySql");
    			if(sql==null) sql=initTask(task);//未被job.split()处理
    			
    			sql = parepareBatchEndSql(cfg,sql);
    			task.set("reader.parameter.ts_querySql",sql);//first init!
    		}
    		if(sql!=null){
	    		task.set("reader.parameter.querySql", //update sql to DELTA query
		    		sql.replace("$ts_start", cfg.getString("job.setting.ts_start"))//ts是全局参数，全部task完成，才更新一次
		    			.replace("$ts_end", cfg.getString("job.setting.ts_end"))
	    			);
    		}
    		
    		//task.set("reader.parameter.ts_end", cfg.getString("job.setting.ts_end"));
    		//task.set("reader.parameter.ts_start", cfg.getString("job.setting.ts_start"));
    	}
    	static java.util.regex.Pattern REGEX_TS_START = java.util.regex.Pattern.compile("(\\w*\\.+\\w*)\\W*ts_start");
    	static String parepareBatchEndSql(Configuration cfg,String sql){
    		if(cfg.getInt("job.setting.ts_batch_mins",0)>0 //不存在ts_end???
    				&& sql.indexOf("$ts_end")<0 && sql.indexOf("$ts_start")>0){
    			
    			Matcher m = REGEX_TS_START.matcher(sql);
    			if(m.find()){
    				String criteria = " and "+m.group(1)+" <= '$ts_end' ";
    				LOG.debug("====> prepare $ts_end sql ===>"+criteria);
    				sql=sql+criteria;
    			}
    		}
    		return sql;
    	}
    	static String initTask(Configuration task){
    		String sql=task.getString("writer.parameter.connection[0].table[0]");
    		String jdbcUrl=task.getString("writer.parameter.connection[0].jdbcUrl");
    		task.set("writer.parameter.table", sql);
    		task.set("writer.parameter.jdbcUrl", jdbcUrl);
    		
    		sql=task.getString("reader.parameter.connection[0].querySql[0]");
    		jdbcUrl=task.getString("reader.parameter.connection[0].jdbcUrl[0]");
    		
    		task.set("reader.parameter.fetchSize", Integer.MIN_VALUE);
    		task.set("reader.parameter.querySql", sql);
    		task.set("reader.parameter.jdbcUrl", jdbcUrl);
    		return sql;
    	}
    	public static void write(Configuration cfg){
    		String file = cfg.getString("job.setting.ts_file","job.ts.txt");
    		
    		toFile(file,cfg.getString("job.setting.ts_end"));

            LOG.info(">>>  $ts_start = {} <<<",cfg.getString("job.setting.ts_end"));
    	}
       static String fromFile(String path){
    		BufferedReader br =null;
    		try{
    			File f = new File(path);
    			if(f.exists())
    			{
	    			br = new BufferedReader(new FileReader(f));
	    			return br.readLine();
    			}else
    			{
    				LOG.warn("failed to load ts_file '{}' !",f.getAbsolutePath());
    			}
    		} catch (Exception e) {
				e.printStackTrace();
    		}finally{
    			if(br!=null)
					try {
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
    		}
    		return null;
    	}
    	static void toFile(String path,String val){
    		BufferedWriter bw=null;
    		try {
    			bw = new BufferedWriter(new FileWriter(path));
				bw.write(val);
			} catch (IOException e) {
				e.printStackTrace();
			}
    		finally{
    			if(bw!=null)
					try {
						bw.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
    		}
    	}
    	static SimpleDateFormat TS_FORMAT=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    	static String getIntervalTime(String start,int mins,int adjust_sec,int sleepSecs){
    		Calendar calc = Calendar.getInstance();
    		
    		Date now=new Date();
    		if(mins<=0){//未设置间隔， 返回Now
    			calc.setTime(now);
    			calc.add(Calendar.SECOND, adjust_sec);
    		}else//返回start后的n个小时
    		{
    			try {
					calc.setTime(TS_FORMAT.parse(start));
				} catch (ParseException e) {
					e.printStackTrace();
					try {
						calc.setTime(new SimpleDateFormat().parse(start));
					} catch (ParseException e1) {
						e1.printStackTrace();
					}
				}
    			calc.add(Calendar.MINUTE, mins);
    			
    			if(calc.getTime().getTime()>now.getTime()){//超过now()， 重置为now()
    				if(sleepSecs>0)
    				{
    					LOG.info("taskGroup wait '{}' seconds for next run ...",sleepSecs);
    	            	
						try {
							Thread.sleep(sleepSecs*1000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						now=new Date();
    				}
    				calc.setTime(now);
        			calc.add(Calendar.SECOND, adjust_sec);
    			}	
    		}
    		
    		return TS_FORMAT.format(calc.getTime());
    	}
    }

    /**
     * TaskExecutor是一个完整task的执行器
     * 其中包括1：1的reader和writer
     */
    class TaskExecutor {
        private Configuration taskConfig;

        private int taskId;

        private int attemptCount;

        private Channel channel;

        private Thread readerThread;

        private Thread writerThread;
        
        private ReaderRunner readerRunner;
        
        private WriterRunner writerRunner;

        /**
         * 该处的taskCommunication在多处用到：
         * 1. channel
         * 2. readerRunner和writerRunner
         * 3. reader和writer的taskPluginCollector
         */
        private Communication taskCommunication;

        public TaskExecutor(Configuration taskConf, int attemptCount) {
            // 获取该taskExecutor的配置
            this.taskConfig = taskConf;
            Validate.isTrue(null != this.taskConfig.getConfiguration(CoreConstant.JOB_READER)
                            && null != this.taskConfig.getConfiguration(CoreConstant.JOB_WRITER),
                    "[reader|writer]的插件参数不能为空!");

            // 得到taskId
            this.taskId = this.taskConfig.getInt(CoreConstant.TASK_ID);
            this.attemptCount = attemptCount;

            /**
             * 由taskId得到该taskExecutor的Communication
             * 要传给readerRunner和writerRunner，同时要传给channel作统计用
             */
            this.taskCommunication = containerCommunicator
                    .getCommunication(taskId);
            Validate.notNull(this.taskCommunication,
                    String.format("taskId[%d]的Communication没有注册过", taskId));
            this.channel = ClassUtil.instantiate(channelClazz,
                    Channel.class, configuration);
            this.channel.setCommunication(this.taskCommunication);

            /**
             * 获取transformer的参数
             */

            List<TransformerExecution> transformerInfoExecs = TransformerUtil.buildTransformerInfo(taskConfig);

            /**
             * 生成writerThread
             */
            writerRunner = (WriterRunner) generateRunner(PluginType.WRITER);
            this.writerThread = new Thread(writerRunner,
                    String.format("%d-%d-%d-writer",
                            jobId, taskGroupId, this.taskId));
            //通过设置thread的contextClassLoader，即可实现同步和主程序不通的加载器
            this.writerThread.setContextClassLoader(LoadUtil.getJarLoader(
                    PluginType.WRITER, this.taskConfig.getString(
                            CoreConstant.JOB_WRITER_NAME)));

            /**
             * 生成readerThread
             */
            readerRunner = (ReaderRunner) generateRunner(PluginType.READER,transformerInfoExecs);
            this.readerThread = new Thread(readerRunner,
                    String.format("%d-%d-%d-reader",
                            jobId, taskGroupId, this.taskId));
            /**
             * 通过设置thread的contextClassLoader，即可实现同步和主程序不通的加载器
             */
            this.readerThread.setContextClassLoader(LoadUtil.getJarLoader(
                    PluginType.READER, this.taskConfig.getString(
                            CoreConstant.JOB_READER_NAME)));
        }

        public void doStart() {
            this.writerThread.start();

            // reader没有起来，writer不可能结束
            if (!this.writerThread.isAlive() || this.taskCommunication.getState() == State.FAILED) {
                throw DataXException.asDataXException(
                        FrameworkErrorCode.RUNTIME_ERROR,
                        this.taskCommunication.getThrowable());
            }

            this.readerThread.start();

            // 这里reader可能很快结束
            if (!this.readerThread.isAlive() && this.taskCommunication.getState() == State.FAILED) {
                // 这里有可能出现Reader线上启动即挂情况 对于这类情况 需要立刻抛出异常
                throw DataXException.asDataXException(
                        FrameworkErrorCode.RUNTIME_ERROR,
                        this.taskCommunication.getThrowable());
            }

        }


        private AbstractRunner generateRunner(PluginType pluginType) {
            return generateRunner(pluginType, null);
        }

        private AbstractRunner generateRunner(PluginType pluginType, List<TransformerExecution> transformerInfoExecs) {
            AbstractRunner newRunner = null;
            TaskPluginCollector pluginCollector;

            switch (pluginType) {
                case READER:
                    newRunner = LoadUtil.loadPluginRunner(pluginType,
                            this.taskConfig.getString(CoreConstant.JOB_READER_NAME));
                    newRunner.setJobConf(this.taskConfig.getConfiguration(
                            CoreConstant.JOB_READER_PARAMETER));

                    pluginCollector = ClassUtil.instantiate(
                            taskCollectorClass, AbstractTaskPluginCollector.class,
                            configuration, this.taskCommunication,
                            PluginType.READER);

                    RecordSender recordSender;
                    if (transformerInfoExecs != null && transformerInfoExecs.size() > 0) {
                        recordSender = new BufferedRecordTransformerExchanger(taskGroupId, this.taskId, this.channel,this.taskCommunication ,pluginCollector, transformerInfoExecs);
                    } else {
                        recordSender = new BufferedRecordExchanger(this.channel, pluginCollector);
                    }

                    ((ReaderRunner) newRunner).setRecordSender(recordSender);

                    /**
                     * 设置taskPlugin的collector，用来处理脏数据和job/task通信
                     */
                    newRunner.setTaskPluginCollector(pluginCollector);
                    break;
                case WRITER:
                    newRunner = LoadUtil.loadPluginRunner(pluginType,
                            this.taskConfig.getString(CoreConstant.JOB_WRITER_NAME));
                    newRunner.setJobConf(this.taskConfig
                            .getConfiguration(CoreConstant.JOB_WRITER_PARAMETER));

                    pluginCollector = ClassUtil.instantiate(
                            taskCollectorClass, AbstractTaskPluginCollector.class,
                            configuration, this.taskCommunication,
                            PluginType.WRITER);
                    ((WriterRunner) newRunner).setRecordReceiver(new BufferedRecordExchanger(
                            this.channel, pluginCollector));
                    /**
                     * 设置taskPlugin的collector，用来处理脏数据和job/task通信
                     */
                    newRunner.setTaskPluginCollector(pluginCollector);
                    break;
                default:
                    throw DataXException.asDataXException(FrameworkErrorCode.ARGUMENT_ERROR, "Cant generateRunner for:" + pluginType);
            }

            newRunner.setTaskGroupId(taskGroupId);
            newRunner.setTaskId(this.taskId);
            newRunner.setRunnerCommunication(this.taskCommunication);

            return newRunner;
        }

        // 检查任务是否结束
        private boolean isTaskFinished() {
            // 如果reader 或 writer没有完成工作，那么直接返回工作没有完成
            if (readerThread.isAlive() || writerThread.isAlive()) {
                return false;
            }

            if(taskCommunication==null || !taskCommunication.isFinished()){
        		return false;
        	}

            return true;
        }
        
        private int getTaskId(){
        	return taskId;
        }

        private long getTimeStamp(){
            return taskCommunication.getTimestamp();
        }

        private int getAttemptCount(){
            return attemptCount;
        }
        
        private boolean supportFailOver(){
        	return writerRunner.supportFailOver();
        }

        private void shutdown(){
            writerRunner.shutdown();
            readerRunner.shutdown();
            if(writerThread.isAlive()){
                writerThread.interrupt();
            }
            if(readerThread.isAlive()){
                readerThread.interrupt();
            }
        }

        private boolean isShutdown(){
            return !readerThread.isAlive() && !writerThread.isAlive();
        }
    }
}
