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

package com.bugull.mq.task;

import com.bugull.mq.utils.MQ;
import com.bugull.mq.listener.BinaryQueueListener;
import com.bugull.mq.utils.BinaryUtil;
import com.bugull.mq.utils.JedisUtil;
import java.util.List;
import redis.clients.jedis.JedisPool;

/**
 *
 * @author Frank Wen(xbwen@hotmail.com)
 */
public class ConsumeBinaryQueueTask extends BlockedTask {
    
    private BinaryQueueListener listener;
    private JedisPool pool;
    private String queue;

    public ConsumeBinaryQueueTask(BinaryQueueListener listener, JedisPool pool, String queue) {
        this.listener = listener;
        this.pool = pool;
        this.queue = queue;
    }

    @Override
    public void run() {
        while(!stopped){
            try{
                jedis = pool.getResource();
                //block until get a message
                List<String> list = jedis.brpop(MQ.BLOCK_TIMEOUT, queue);
                if(list!=null && list.size()==2){
                    byte[] msgId = (MQ.MSG_ID + list.get(1)).getBytes(MQ.CHARSET);
                    byte[] msg = jedis.get(msgId);
                    if(msg != null){
                        jedis.del(msgId);
                        synchronized(listener){
                            listener.onQueueMessage(queue, msg);
                        }
                    }
                }
            }catch(Exception ex){
                ex.printStackTrace();
            }finally{
                JedisUtil.returnToPool(pool, jedis);
            }
        }
    }

}
