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

import redis.clients.jedis.JedisPubSub;

/**
 *
 * @author Frank Wen(xbwen@hotmail.com)
 */
public abstract class DirectTopicListener extends JedisPubSub {
    
    public abstract void onTopicMessage(String topic, String message);
    
    public abstract void onPatternMessage(String pattern, String topic, String message);
    
    @Override
    public void onMessage(String channel, String message){
        if(message!=null && !message.equals(MQ.EMPTY_MESSAGE)){
            onTopicMessage(channel, message);
        }
    }
    
    @Override
    public void onPMessage(String pattern, String channel, String message){
        onPatternMessage(pattern, channel, message);
    }
    
    @Override
    public void onSubscribe(String channel, int subscribedChannels){
        //do nothing
    }

    @Override
    public void onUnsubscribe(String channel, int subscribedChannels){
        //do nothing
    }

    @Override
    public void onPUnsubscribe(String pattern, int subscribedChannels){
        //do nothing
    }

    @Override
    public void onPSubscribe(String pattern, int subscribedChannels){
        //do nothing
    }

}
