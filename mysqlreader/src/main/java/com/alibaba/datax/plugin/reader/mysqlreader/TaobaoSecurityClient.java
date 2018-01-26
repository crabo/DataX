package com.alibaba.datax.plugin.reader.mysqlreader;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.plugin.reader.mysqlreader.DecryptRdbmsReader.Task;
import com.taobao.api.Constants;
import com.taobao.api.SecretException;
import com.taobao.api.internal.util.Base64;
import com.taobao.api.internal.util.StringUtils;
import com.taobao.api.internal.util.TaobaoUtils;
import com.taobao.api.internal.util.WebUtils;
import com.taobao.api.internal.util.json.JSONValidatingReader;
import com.taobao.api.security.SecretContext;
import com.taobao.api.security.SecretData;
import com.taobao.api.security.SecurityBiz;
import com.taobao.api.security.SecurityConstants;

public class TaobaoSecurityClient implements SecurityConstants{
	private static Map<String,SecretContext> CACHED_SECRET_CONTEXT
		= new ConcurrentHashMap<String,SecretContext>();
	
	private static Map<String,List<SecretContext>> CACHED_BACKUP_SECRET_KEYS
		= new ConcurrentHashMap<String,List<SecretContext>>();
	
	private static final Logger LOG = LoggerFactory
            .getLogger(TaobaoSecurityClient.class);
	private final String serverUrl;
	//private final Long secretVersion;
	
	public TaobaoSecurityClient(String url){
		this.serverUrl=url; 
		//this.secretVersion = version;
	}
	/**
     * 从本地获取秘钥信息
     * 
     * @param session
     * @param cacheKey
     * @return
     */
    private SecretContext getSecret(String shopId,Long version) {
    	String key = shopId+Math.abs(version);
    	SecretContext secretContext = CACHED_SECRET_CONTEXT.get(key);
    	if(secretContext==null)
    	{
    		//请求时，参数允许为负数， 代表公共密钥： -1
    		String url = this.serverUrl.replace("$version", version.toString());
    		LOG.info("request secret keys from server: "+url);
    		
    		synchronized (CACHED_SECRET_CONTEXT){
    			if(!CACHED_SECRET_CONTEXT.containsKey(key))//double check!
    			{
		    		try {
						refreshSecretsFromServer(shopId,url);
					} catch (SecretException |IOException e) {
						throw new IllegalArgumentException("request secret keys error on '"+shopId+"'\n"
		            			+url);
					}
    			}
    		}
    		//确认已获取到secretKey
            secretContext = CACHED_SECRET_CONTEXT.get(key);
            if(secretContext==null)//防呆： 服务端密钥返回仍然是负数?
            	secretContext = CACHED_SECRET_CONTEXT.get(shopId+version);
            if(secretContext==null)
            	throw new IllegalArgumentException("decrypt version '"+version+"' is not found:\n"
            			+url);
    	}
        return secretContext;
    }
    private void refreshSecretsFromServer(String shopId,String url) throws SecretException, IOException{
    	String resp = WebUtils.doPost(url, null,12000,10000);
    	String decryptKey;
    	if(shopId.length()>15)
    		decryptKey=shopId.substring(0, 16);
    	else
    		decryptKey=String.format("%-16s", shopId).replace(' ', '0');
    	
    	LOG.info("got secret response from server: {}\n{}",url,resp);
    	Map<Object, Object> result = (Map<Object, Object>)new JSONValidatingReader().read(resp);
    	
    	if("非淘宝店铺!".equals(result.get("Description")))
    	{
    		LOG.info("shop '{}' is not TOP encrypted.",shopId);
    		return;
    	}
    	if(result.get("ReturnValue")==null){
    		throw new SecretException("Invalid secret ReturnValue: "+result.get("Description"));
    	}
    	
    	resp = TaobaoUtils.aesDecrypt(result.get("ReturnValue").toString(), 
    			decryptKey.getBytes(Constants.CHARSET_UTF8));
    	
    	Map<Object, Object> ret;
    	try
    	{
    		ret = (Map<Object, Object>)new JSONValidatingReader().read(resp);
    	}catch(Exception ex)
    	{
    		LOG.error("Invalid secret key format:\n{}",resp);
    		throw new SecretException("Invalid secret key format",ex);
    	}
    	List<Object> li = (List<Object>)ret.get("secretList");
    	
    	
    	for(Object obj : li){
    		Map<Object, Object> item = (Map<Object, Object>)obj;
    		
    		SecretContext secretContext = new SecretContext();
    		Long version = Long.valueOf(item.get("SecretVersion").toString());
    		long currentTime = System.currentTimeMillis();
            secretContext.setInvalidTime(currentTime + (864000 * 1000));
            secretContext.setMaxInvalidTime(currentTime + (864000 * 1000));
            secretContext.setSecret(Base64.decode(item.get("Secret").toString()));
            secretContext.setSecretVersion(version);
            //AppConfig 
            //Response 
            String key = shopId+version;//update if exists
            CACHED_SECRET_CONTEXT.put(key, secretContext);
            
            if(version<1){
            	List<SecretContext> backupKeys=CACHED_BACKUP_SECRET_KEYS.get(shopId);
            	if(backupKeys==null){
            		backupKeys=new ArrayList<SecretContext>();
            		CACHED_BACKUP_SECRET_KEYS.put(shopId,backupKeys);
            	}
            	backupKeys.add(secretContext);
            }
    	}
    	LOG.info(" '{}' secret keys loaded from server.",CACHED_SECRET_CONTEXT.size());
    }
    
    
	public String decrypt(String data, String type, String shopId) throws SecretException {
        if (StringUtils.isEmpty(data) || data.length() < 4) {
            return data;
        }

        // 获取分隔符
        Character charValue = SecurityBiz.getSeparatorCharMap().get(type);
        if (charValue == null) {
            throw new SecretException("type error");
        }

        // 校验
        char separator = charValue.charValue();
        if (!(data.charAt(0) == separator && data.charAt(data.length() - 1) == separator)) {
            return data;
        }
        SecretData secretDataDO = null;
        if (data.charAt(data.length() - 2) == separator) {
            secretDataDO = SecurityBiz.getIndexSecretData(data, separator);
        } else {
            secretDataDO = SecurityBiz.getSecretData(data, separator);
        }

        // 非法密文
        if (secretDataDO == null) {
            return data;
        }
        //try {
            //SecurityCounter.addDecryptCount(type);// 计数器
            SecretContext secretContextDO = this.getSecret(shopId,secretDataDO.getSecretVersion());
            //TaobaoUtils.aesDecrypt(secretDataDO.getOriginalBase64Value(), secretContextDO.getSecret());
            String decryptValue = decryptWithTryCatch(shopId,secretContextDO,secretDataDO);
            if(decryptValue==null){
            	List<SecretContext> backupKeys=CACHED_BACKUP_SECRET_KEYS.get(shopId);
            	for(SecretContext ctxDo : backupKeys){
            		decryptValue = decryptWithTryCatch(shopId,ctxDo,secretDataDO);
            		if(decryptValue!=null)
            			break;
            	}
            	if(decryptValue==null)
            		throw new SecretException("can not decrypt with all '"+backupKeys.size()+"' backup keys");
            }
            		
            if (PHONE.equals(type) && !secretDataDO.isSearch()) {
                // 加上手机号前3位，手机号只加密了后8位
                return secretDataDO.getOriginalValue() + decryptValue;
            }
            return decryptValue;
        /*} catch (ApiException e) {
            throw new SecretException("get secret error", e);
        }*/
    }
	
	private String decryptWithTryCatch(String shopId,SecretContext secretContextDO,SecretData secretDataDO){
		try{
			return TaobaoUtils.aesDecrypt(secretDataDO.getOriginalBase64Value(), secretContextDO.getSecret());
		}catch(SecretException ex){
			LOG.info(" decrypt attempt failed!  "+ex.toString());
			return null;
		}
	}
}
