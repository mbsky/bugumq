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

package com.bugull.mq.listener;

import com.bugull.mq.Client;
import com.bugull.mq.Connection;
import com.bugull.mq.MQ;
import com.bugull.mq.exception.MQException;
import com.bugull.mq.utils.StringUtil;
import com.bugull.mq.utils.JedisUtil;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

/**
 * Listener to receive topic message.
 * 
 * @author Frank Wen(xbwen@hotmail.com)
 */
public abstract class TopicListener extends JedisPubSub {
    
    protected ConcurrentMap<String, ScheduledFuture> map = new ConcurrentHashMap<String, ScheduledFuture>();
    
    protected ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    
    /**
     * use a timer to re-subscribe
     * @param topic
     * @param pattern 
     */
    public void addTimer(final String topic, final boolean isCommonTopic){
        Runnable task = new Runnable(){
            @Override
            public void run() {
                map.remove(topic);
                Client client = Connection.getInstance().getClient();
                if(isCommonTopic){
                    try{
                        client.unsubscribe(topic);
                    }catch(MQException ex){
                        
                    }
                    client.subscribe(topic);
                }else{
                    try{
                        client.unsubscribePattern(topic);
                    }catch(MQException ex){
                        
                    }
                    client.subscribePattern(topic);
                }
            }
        };
        ScheduledFuture future = scheduler.schedule(task, MQ.SUBSCRIBE_TIMEOUT, TimeUnit.SECONDS);
        ScheduledFuture temp = map.putIfAbsent(topic, future);
        if(temp != null){
            future.cancel(true);
        }
    }
    
    public void removeTimer(String topic){
        ScheduledFuture future = map.remove(topic);
        if(future != null){
            future.cancel(true);
        }
    }
    
    public void closeAllTimer(){
        scheduler.shutdownNow();
    }
    
    public abstract void onTopicMessage(String topic, String message);
    
    public abstract void onPatternMessage(String pattern, String topic, String message);
    
    @Override
    public void onMessage(String channel, String message){
        synchronized(this){
            onTopicMessage(channel, message);
        }
    }
    
    @Override
    public void onPMessage(String pattern, String channel, String message){
        synchronized(this){
            onPatternMessage(pattern, channel, message);
        }
    }
    
    @Override
    public void onSubscribe(String channel, int subscribedChannels){
        removeTimer(channel);
        Connection conn = Connection.getInstance();
        JedisPool pool = conn.getPool();
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            String retainMessage = jedis.get(MQ.RETAIN + channel);
            if(!StringUtil.isNull(retainMessage)){
                synchronized(this){
                    onTopicMessage(channel, retainMessage);
                }
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    @Override
    public void onPSubscribe(String pattern, int subscribedChannels){
        removeTimer(pattern);
    }

    @Override
    public void onUnsubscribe(String channel, int subscribedChannels){
        //do nothing
    }

    @Override
    public void onPUnsubscribe(String pattern, int subscribedChannels){
        //do nothing
    }

}
