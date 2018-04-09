package com.cnipr.myredis;

import redis.clients.jedis.Jedis;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	Jedis jedis = new Jedis("192.168.242.130", 6379);
    	jedis.auth("foobared");
//    	jedis.flushAll();
//    	jedis.set("foo", "bar23");
//    	long count = jedis.dbSize();
    	String value = jedis.get("foo");        
    	System.out.println(value);
//    	System.out.println(count);
    }
}
