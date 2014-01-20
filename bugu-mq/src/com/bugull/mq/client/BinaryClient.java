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
import com.bugull.mq.listener.BinaryQueueListener;
import com.bugull.mq.listener.BinaryTopicListener;
import com.bugull.mq.task.ConsumeBinaryQueueTask;
import com.bugull.mq.task.SubscribeBinaryTopicTask;
import com.bugull.mq.utils.BinaryUtil;
import com.bugull.mq.utils.JedisUtil;
import com.bugull.mq.utils.StringUtil;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

/**
 *
 * @author Frank Wen(xbwen@hotmail.com)
 */
public class BinaryClient extends AbstractClient {
    
    private BinaryTopicListener topicListener;

    public BinaryClient(JedisPool pool){
        this.pool = pool;
    }
    
    public void publish(String topic, byte[] message) throws MQException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            jedis.publish(topic.getBytes(MQ.CHARSET), message);
        }catch(Exception ex){
            //Note: catch Exception here, because there are many runtime exception in Jedis.
            //Following code is same like this.
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void publishRetain(String topic, byte[] message) throws MQException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            Transaction tx = jedis.multi();
            tx.publish(topic.getBytes(MQ.CHARSET), message);
            tx.set((MQ.RETAIN + topic).getBytes(MQ.CHARSET), message);
            tx.exec();
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void clearRetainMessage(String... topics) throws MQException {
        Jedis jedis = null;
        int len = topics.length;
        byte[][] keys = new byte[len][];
        try{
            for(int i=0; i< len; i++){
                keys[i] = (MQ.RETAIN + topics[i]).getBytes(MQ.CHARSET);
            }
            jedis = pool.getResource();
            jedis.del(keys);
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void subscribe(String... topics) {
        String key = StringUtil.concat(topics);
        ExecutorService es = topicServices.get(key);
        if(es == null){
            es = Executors.newSingleThreadExecutor();
            ExecutorService temp = topicServices.putIfAbsent(key, es);
            if(temp == null){
                SubscribeBinaryTopicTask task = new SubscribeBinaryTopicTask(topicListener, pool, BinaryUtil.toBytes(topics));
                es.execute(task);
                blockedTasks.putIfAbsent(key, task);
            }
        }
        for(String topic : topics){
            topicListener.addTimer(topic);
        }
    }
    
    public void unsubscribe(String... topics) throws MQException {
        try{
            topicListener.unsubscribe(BinaryUtil.toBytes(topics));
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }
        for(String topic : topics){
            topicListener.removeTimer(topic);
        }
    }
    
    public void produce(String queue, byte[]... messages) throws MQException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            for(byte[] msg : messages){
                long count = jedis.incr(MQ.MSG_COUNT);
                String id = String.valueOf(count);
                byte[] msgId = (MQ.MSG_ID + id).getBytes(MQ.CHARSET);
                Transaction tx = jedis.multi();
                tx.lpush(queue, id);
                tx.set(msgId, msg);
                tx.exec();
            }
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void produce(String queue, int expire, byte[]... messages) throws MQException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            for(byte[] msg : messages){
                long count = jedis.incr(MQ.MSG_COUNT);
                String id = String.valueOf(count);
                byte[] msgId = (MQ.MSG_ID + id).getBytes(MQ.CHARSET);
                Transaction tx = jedis.multi();
                tx.lpush(queue, id);
                tx.set(msgId, msg);
                tx.expire(msgId, expire);
                tx.exec();
            }
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void produce(String queue, Date expireAt, byte[]... messages) throws MQException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            for(byte[] msg : messages){
                long count = jedis.incr(MQ.MSG_COUNT);
                String id = String.valueOf(count);
                byte[] msgId = (MQ.MSG_ID + id).getBytes(MQ.CHARSET);
                Transaction tx = jedis.multi();
                tx.lpush(queue, id);
                tx.set(msgId, msg);
                tx.expireAt(msgId, expireAt.getTime());
                tx.exec();
            }
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void produceUrgency(String queue, byte[]... messages) throws MQException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            for(byte[] msg : messages){
                long count = jedis.incr(MQ.MSG_COUNT);
                String id = String.valueOf(count);
                byte[] msgId = (MQ.MSG_ID + id).getBytes(MQ.CHARSET);
                Transaction tx = jedis.multi();
                tx.rpush(queue, id);
                tx.set(msgId, msg);
                tx.exec();
            }
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void produceUrgency(String queue, int expire, byte[]... messages) throws MQException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            for(byte[] msg : messages){
                long count = jedis.incr(MQ.MSG_COUNT);
                String id = String.valueOf(count);
                byte[] msgId = (MQ.MSG_ID + id).getBytes(MQ.CHARSET);
                Transaction tx = jedis.multi();
                tx.rpush(queue, id);
                tx.set(msgId, msg);
                tx.expire(msgId, expire);
                tx.exec();
            }
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void produceUrgency(String queue, Date expireAt, byte[]... messages) throws MQException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            for(byte[] msg : messages){
                long count = jedis.incr(MQ.MSG_COUNT);
                String id = String.valueOf(count);
                byte[] msgId = (MQ.MSG_ID + id).getBytes(MQ.CHARSET);
                Transaction tx = jedis.multi();
                tx.rpush(queue, id);
                tx.set(msgId, msg);
                tx.expireAt(msgId, expireAt.getTime());
                tx.exec();
            }
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void consume(BinaryQueueListener listener, String... queues){
        for(String queue : queues){
            ExecutorService es = queueServices.get(queue);
            if(es == null){
                es = Executors.newSingleThreadExecutor();
                ExecutorService temp = queueServices.putIfAbsent(queue, es);
                if(temp == null){
                    ConsumeBinaryQueueTask task = new ConsumeBinaryQueueTask(listener, pool, queue);
                    es.execute(task);
                    blockedTasks.putIfAbsent(queue, task);
                }
            }
        }
    }
    
    public void clearQueue(String... queues) throws MQException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            for(String queue : queues){
                long size = jedis.llen(queue);
                for(long i=0; i<size; i++){
                    String id = jedis.rpop(queue);
                    if(!StringUtil.isNull(id)){
                        jedis.del((MQ.MSG_ID + id).getBytes(MQ.CHARSET));
                    }
                }
            }
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void retainQueue(String queue, long retainSize) throws MQException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            long size = jedis.llen(queue);
            long count = size - retainSize;
            for(long i=0; i<count; i++){
                String id = jedis.rpop(queue);
                if(!StringUtil.isNull(id)){
                    jedis.del((MQ.MSG_ID + id).getBytes(MQ.CHARSET));
                }
            }
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }

    public void setTopicListener(BinaryTopicListener topicListener) {
        this.topicListener = topicListener;
    }
    
    @Override
    public void stopAllTopicTask(){
        super.stopAllTopicTask();
        if(topicListener != null){
            topicListener.closeAllTimer();
        }
    }

}
