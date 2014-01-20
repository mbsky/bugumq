/*
 * Copyright (c) www.bugull.com
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bugull.mq.utils;

import com.bugull.mq.Connection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 *
 * @author Frank Wen(xbwen@hotmail.com)
 */
public final class ClientUtil {
    
    public static byte[] getData(byte[] key){
        byte[] result = null;
        JedisPool pool = Connection.getInstance().getPool();
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            result = jedis.get(key);
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
        return result;
    }
    
    public static void setData(byte[] key, byte[] data){
        JedisPool pool = Connection.getInstance().getPool();
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            jedis.set(key, data);
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public static void delData(byte[] key){
        JedisPool pool = Connection.getInstance().getPool();
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            jedis.del(key);
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public static String getData(String key){
        String result = null;
        JedisPool pool = Connection.getInstance().getPool();
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            result = jedis.get(key);
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
        return result;
    }
    
    public static void setData(String key, String data){
        JedisPool pool = Connection.getInstance().getPool();
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            jedis.set(key, data);
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public static void delData(String key){
        JedisPool pool = Connection.getInstance().getPool();
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            jedis.del(key);
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }

}
