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

package com.bugull.mq;

import java.util.Set;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

/**
 * Listener to receive topic message.
 * 
 * @author Frank Wen(xbwen@hotmail.com)
 */
public abstract class TopicListener extends JedisPubSub {
    
    public abstract void onTopicMessage(String topic, String message);
    
    public abstract void onPatternMessage(String pattern, String topic, String message);
    
    @Override
    public void onMessage(String channel, String message){
        synchronized(this){
            if(message!=null && !message.equals(MQ.EMPTY_MESSAGE)){
                onTopicMessage(channel, message);
            }
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
        Connection conn = Connection.getInstance();
        JedisPool pool = conn.getPool();
        Jedis jedis = pool.getResource();
        String retainMessage = jedis.get(MQ.TOPIC + channel);
        if(!StringUtil.isNull(retainMessage)){
            synchronized(this){
                onTopicMessage(channel, retainMessage);
            }
        }
        pool.returnResource(jedis);
    }
    
    @Override
    public void onPSubscribe(String pattern, int subscribedChannels){
        Connection conn = Connection.getInstance();
        JedisPool pool = conn.getPool();
        Jedis jedis = pool.getResource();
        int len = MQ.TOPIC.length();
        Set<String> set = jedis.keys(MQ.TOPIC + pattern);
        for(String s : set){
            String retainMessage = jedis.get(s);
            if(!StringUtil.isNull(retainMessage)){
                synchronized(this){
                    onPatternMessage(pattern, s.substring(len), retainMessage);
                }
            }
        }
        pool.returnResource(jedis);
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
