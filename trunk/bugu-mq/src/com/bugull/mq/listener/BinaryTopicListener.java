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

import redis.clients.jedis.BinaryJedisPubSub;

/**
 *
 * @author Frank Wen(xbwen@hotmail.com)
 */
public abstract class BinaryTopicListener extends BinaryJedisPubSub {
    
    public abstract void onBinaryMessage(String topic, byte[] message);
    
    @Override
    public void onMessage(byte[] channel, byte[] message){
        if(channel==null || message==null){
            return;
        }
        synchronized(this){
            onBinaryMessage(new String(channel), message);
        }
    }

    @Override
    public void onPMessage(byte[] pattern, byte[] channel, byte[] message){
        
    }

    @Override
    public void onSubscribe(byte[] channel, int subscribedChannels){
        
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
