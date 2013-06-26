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

import java.util.List;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * A thread to consume a queue.
 * 
 * @author Frank Wen(xbwen@hotmail.com)
 */
public class ConsumeQueueTask implements Runnable {
    
    private QueueListener listener;
    private JedisPool pool;
    private String queue;

    public ConsumeQueueTask(QueueListener listener, JedisPool pool, String queue) {
        this.listener = listener;
        this.pool = pool;
        this.queue = queue;
    }

    @Override
    public void run() {
        while(true){
            Jedis jedis = pool.getResource();
            //block until get a message
            List<String> list = jedis.brpop(0, queue);
            if(list!=null && list.size()==2){
                String msgId = MQ.MSG_ID + list.get(1);
                String msg = jedis.get(msgId);
                if(msg != null){
                    jedis.del(msgId);
                    listener.onQueueMessage(queue, msg);
                }
            }
            pool.returnResource(jedis);
        }
    }

}
