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
import java.io.UnsupportedEncodingException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import redis.clients.jedis.BinaryJedisPubSub;

/**
 *
 * @author Frank Wen(xbwen@hotmail.com)
 */
public abstract class BinaryTopicListener extends BinaryJedisPubSub {
    
    protected ConcurrentMap<String, ScheduledFuture> map = new ConcurrentHashMap<String, ScheduledFuture>();
    
    protected ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    
    /**
     * use a timer to re-subscribe
     * @param topic
     * @param pattern 
     */
    public void addTimer(final String topic){
        Runnable task = new Runnable(){
            @Override
            public void run() {
                map.remove(topic);
                Client client = Connection.getInstance().getClient();
                try{
                    client.unsubscribeFileBroadcast(topic);
                }catch(MQException ex){

                }
                client.subscribeFileBroadcast(topic);
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
    
    public abstract void onBinaryMessage(String topic, byte[] message);
    
    @Override
    public void onMessage(byte[] channel, byte[] message){
        if(channel==null || message==null){
            return;
        }
        String s = null;
        try{
            s = new String(channel, MQ.CHARSET);
        }catch(UnsupportedEncodingException ex){
            
        }
        synchronized(this){
            onBinaryMessage(s, message);
        }
    }

    @Override
    public void onPMessage(byte[] pattern, byte[] channel, byte[] message){
        
    }

    @Override
    public void onSubscribe(byte[] channel, int subscribedChannels){
        String s = null;
        try{
            s = new String(channel, MQ.CHARSET);
        }catch(UnsupportedEncodingException ex){
            
        }
        removeTimer(s);
    }

    @Override
    public void onUnsubscribe(byte[] channel, int subscribedChannels){
        
    }

    @Override
    public void onPUnsubscribe(byte[] pattern, int subscribedChannels){
        
    }

    @Override
    public void onPSubscribe(byte[] pattern, int subscribedChannels){
        
    }

}
