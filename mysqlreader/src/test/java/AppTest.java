
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;


import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.datax.plugin.reader.mysqlreader.TaobaoSecurityClient;
import com.taobao.api.SecretException;
import com.taobao.api.internal.util.Base64;
import com.taobao.api.internal.util.TaobaoUtils;
import com.taobao.api.security.SecurityConstants;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;


/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
	final String shopId;
	final String secretKey;
	Map<String,String> PlainWithCrypt;
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
    	super( testName );
    	shopId = "fxfallinmissqjd";
    	secretKey = "ED+++LHkQbiQ69UfxGYOTA==";
    	
    	PlainWithCrypt = new HashMap<String,String>();
    	PlainWithCrypt.put("139", "$139$mfsPpcplTw4qnuzycUh6kg==$-1$");
    	PlainWithCrypt.put("135", "$135$9Z647vwRUzcj4aQX7ZAfAQ==$-1$");
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }
    
    public void testCypto() throws IOException, SecretException{
    	TaobaoSecurityClient client = new TaobaoSecurityClient("http://ryweb.kedaocrm.com/SyncData/GetShopDataSecretInfo?dbName="+this.shopId+"&version=1");
    	for(Entry<String,String> kv : PlainWithCrypt.entrySet())
    	{
    		System.out.println(kv.getKey()+" = "+
    				client.decrypt(kv.getValue(), 
    						kv.getKey().startsWith("1")?"phone":"simple", this.shopId)
    				);
    	}
    	
    }
    
    public void testCyptoAes() throws UnsupportedEncodingException, SecretException{
    	String data = "Mar2R6GoC3cFMLTlDWP7hM8ofK6UVagLTjgac0AZvOU=";
    				   
    	String key = "a_bc*&00000000000000000000000000";
    	String base64 = Base64.encodeToString(key.getBytes("utf-8"), false);
    	
    	printHexString(Base64.decode(base64));
    	System.out.println(
    			TaobaoUtils.aesDecrypt(data,Base64.decode(base64))
    			);
    }
    
    public static void printHexString(byte[] b)    
    {    
        for (int i = 0; i < b.length; i++)    
        {    
            String hex = Integer.toHexString(b[i] & 0xFF);    
            if (hex.length() == 1)    
            {    
                hex = '0' + hex;    
            }    
            System.out.print(hex.toUpperCase() + " ");    
        }    
        System.out.println("");    
    }  
    
    public void testShopCypto() throws IOException, SecretException{
    	String shopId="ruxiqjd";
    	String data="~MYDp8RwlM1g+o6aJCJmnR3SdAXUtEok9NBX2d7u/w1E=~1~";
    	
    	TaobaoSecurityClient client = new TaobaoSecurityClient("http://ryweb.kedaocrm.com/SyncData/GetShopDataSecretInfo?dbName="+shopId+"&version=1");
    	
    	System.out.print(
    			client.decrypt(data, data.startsWith("$")?"phone":"simple", shopId)
    	);
    }
    
    public void testShopCyptoLocal() throws IOException, SecretException{
    	String shopId="ruxiqjd";
    	String data="~MYDp8RwlM1g+o6aJCJmnR3SdAXUtEok9NBX2d7u/w1E=~1~";
    	
    	
    	String base64 = Base64.encodeToString("bdBobcRiXFvSNEkkIj+cWg=="
    			.getBytes("utf-8"), false);
    	System.out.println(
    			TaobaoUtils.aesDecrypt(data,Base64.decode(base64))
    			);
    }
    
    public void testMixed(){
    	String[] datas=new String[]{
    			"6618101000~Jkyxebe7On8XFhIFrplIKg==~1~cou",
    			"636120501295474123uaugl002~x1hCRAeq4NsPhUvZofAL2Q==~1~eco",
    			"~n1Q1tEkzAHJ39/MYtJmJtg==~1~49e3dbc4-bfce-4041-8b7e-13721d5acd1dque",
    			"~r8Kuq2MKVBw02FkJtiCIsA==~1~160812114213716"
    	};
    	for(String encyptString : datas)
    	{
	    	if(encyptString.indexOf(SecurityConstants.SIMPLE_SEPARATOR)>-1){
				if(encyptString.endsWith(SecurityConstants.SIMPLE_SEPARATOR))
					System.out.println("===error=====!!!!");
				else{
					int start = encyptString.indexOf(SecurityConstants.SIMPLE_SEPARATOR);
					int end =   encyptString.lastIndexOf(SecurityConstants.SIMPLE_SEPARATOR);
					String part = encyptString.substring(start, end+1);
					String plain = "[DATA]";
					System.out.println("===>"+ encyptString.replace(part, plain));
				}
			}
    	}
    }
    
}
