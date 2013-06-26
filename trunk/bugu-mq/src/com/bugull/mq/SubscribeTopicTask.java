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
 * Thread to receive topic message.
 * 
 * @author Frank Wen(xbwen@hotmail.com)
 */
public class SubscribeTopicTask implements Runnable {
    
    private TopicListener listener;
    private JedisPool pool;
    private String[] topics;

    public SubscribeTopicTask(TopicListener listener, JedisPool pool, String[] topics) {
        this.listener = listener;
        this.pool = pool;
        this.topics = topics;
    }

    @Override
    public void run() {
        Jedis jedis = pool.getResource();
        jedis.subscribe(listener, topics);
        pool.returnResource(jedis);
    }

}
