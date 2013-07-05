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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Thread to receive pattern message.
 * 
 * @author Frank Wen(xbwen@hotmail.com)
 */
public class SubscribePatternTask extends BlockedTask {
    
    private TopicListener listener;
    private JedisPool pool;
    private String[] patterns;

    public SubscribePatternTask(TopicListener listener, JedisPool pool, String[] patterns) {
        this.listener = listener;
        this.pool = pool;
        this.patterns = patterns;
    }

    @Override
    public void run() {
        Jedis j = pool.getResource();
        this.jedis = j;
        
        try{
            //the psubscribe method is blocked.
            j.psubscribe(listener, patterns);
        }catch(Exception ex){
            //ignore the exception
        }
        
        pool.returnResource(j);
    }

}
