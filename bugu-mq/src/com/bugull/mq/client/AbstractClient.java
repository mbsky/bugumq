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

package com.bugull.mq.client;

import com.bugull.mq.utils.MQ;
import com.bugull.mq.exception.MQException;
import com.bugull.mq.task.BlockedTask;
import com.bugull.mq.utils.JedisUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

/**
 *
 * @author Frank Wen(xbwen@hotmail.com)
 */
public abstract class AbstractClient {
    
    protected JedisPool pool;
    
    //store the blocked tasks, in order to stop it and close the jedis client.
    protected ConcurrentMap<String, BlockedTask> blockedTasks = new ConcurrentHashMap<String, BlockedTask>();
    
    protected ConcurrentMap<String, ExecutorService> queueServices = new ConcurrentHashMap<String, ExecutorService>();
    protected ConcurrentMap<String, ExecutorService> topicServices = new ConcurrentHashMap<String, ExecutorService>();
    
    public boolean isOnline(String clientId) throws MQException {
        boolean result = false;
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            result = jedis.exists(MQ.ONLINE + clientId);
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
        return result;
    }
    
    public List<Boolean> isOnline(List<String> clientList) throws MQException {
        List<Boolean> results = new ArrayList<Boolean>();
        List<Response<Boolean>> responseList = new ArrayList<Response<Boolean>>();
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            responseList = new ArrayList<Response<Boolean>>();
            Pipeline p = jedis.pipelined();
            for(String clientId : clientList){
                responseList.add(p.exists(MQ.ONLINE + clientId));
            }
            p.sync();
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
        for(Response<Boolean> response : responseList){
            results.add(response.get());
        }
        return results;
    }
    
    public void clearAll() throws MQException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            jedis.flushDB();
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public long getQueueSize(String queue) throws MQException {
        long size = 0;
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            size = jedis.llen(queue);
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
        return size;
    }
    
    public void stopConsume(String... queues){
        for(String queue : queues){
            BlockedTask task = blockedTasks.get(queue);
            if(task != null){
                task.setStopped(true);
                try{
                    task.getJedis().disconnect();
                }catch(Exception ex){
                    //ignore ex
                }
                blockedTasks.remove(queue);
            }
            ExecutorService es = queueServices.get(queue);
            if(es != null){
                es.shutdownNow();
                queueServices.remove(queue);
            }
        }
    }
    
    public void stopAllConsume(){
        Set<String> set = queueServices.keySet();
        for(String queue : set){
            stopConsume(queue);
        }
    }
    
    protected void stopAllTopicTask(){
        Set<String> set = topicServices.keySet();
        for(String topic : set){
            BlockedTask task = blockedTasks.get(topic);
            if(task != null){
                task.setStopped(true);
                try{
                    task.getJedis().disconnect();
                }catch(Exception ex){
                    //ignore ex
                }
                blockedTasks.remove(topic);
            }
            ExecutorService es = topicServices.get(topic);
            if(es != null){
                es.shutdownNow();
                topicServices.remove(topic);
            }
        }
    }

}
