
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

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;


/**
 * Unit test for simple App.
 */
public class JdSecret 
    extends TestCase
{
	final String secretKey;
	Map<String,String> PlainWithCrypt;
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public JdSecret( String testName )
    {
    	super( testName );
    	secretKey = "6lhKbAu+8M3y9wODOwR6TA==";
    	
    	PlainWithCrypt = new HashMap<String,String>();
    	PlainWithCrypt.put("xmkp11", "~t4svwVdUx8g80l45wAVexg==~1~");
    	PlainWithCrypt.put("mobile", "$134$yG5U0E2xtRJUoyyeYV7n0A==$1$");
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( JdSecret.class );
    }
    
    public void testCypto() throws IOException, SecretException{
    	TaobaoSecurityClient client = new TaobaoSecurityClient("http://ryweb.kedaocrm.com/SyncData/GetShopDataSecretInfo?dbName=jdaehm&version=1");
    	for(Entry<String,String> kv : PlainWithCrypt.entrySet())
    	{
    		System.out.println(kv.getKey()+" = "+
    				client.decrypt(kv.getValue(), 
    						kv.getValue().startsWith("$")?"phone":"simple", "jdaehm")
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
    
}
