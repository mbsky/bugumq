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

import java.util.Date;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

/**
 * Presents an MQ client. All MQ operation is implemented here.
 * 
 * @author Frank Wen(xbwen@hotmail.com)
 */
public class Client {
    
    private JedisPool pool;
    
    private TopicListener topicListener;
    
    private final ConcurrentMap<String, ExecutorService> queueServices = new ConcurrentHashMap<String, ExecutorService>();
    private final ConcurrentMap<String, ExecutorService> topicServices = new ConcurrentHashMap<String, ExecutorService>();
    
    //store the blocked tasks, in order to close the jedis client.
    private final ConcurrentMap<String, BlockedTask> blockedTasks = new ConcurrentHashMap<String, BlockedTask>();
    
    public Client(JedisPool pool){
        this.pool = pool;
    }
    
    public void publish(String topic, String message){
        Jedis jedis = pool.getResource();
        jedis.publish(topic, message);
        pool.returnResource(jedis);
    }
    
    public void publishRetain(String topic, String message){
        Jedis jedis = pool.getResource();
        Transaction tx = jedis.multi();
        tx.publish(topic, message);
        tx.set(MQ.TOPIC + topic, message);
        tx.exec();
        pool.returnResource(jedis);
    }
    
    public void clearRetainMessage(String... topics){
        int len = topics.length;
        String[] keys = new String[len];
        for(int i=0; i< len; i++){
            keys[i] = MQ.TOPIC + topics[i];
        }
        Jedis jedis = pool.getResource();
        jedis.del(keys);
        pool.returnResource(jedis);
    }
    
    public void subscribe(String... topics) throws NoTopicListenerException{
        if(topicListener == null){
            throw new NoTopicListenerException("No TopicListener is set");
        }
        String key = StringUtil.concat(topics);
        ExecutorService es = topicServices.get(key);
        if(es == null){
            es = Executors.newSingleThreadExecutor();
            ExecutorService temp = topicServices.putIfAbsent(key, es);
            if(temp == null){
                SubscribeTopicTask task = new SubscribeTopicTask(topicListener, pool, topics);
                es.execute(task);
                blockedTasks.putIfAbsent(key, task);
            }
        }
    }
    
    public void subscribePattern(String... patterns) throws NoTopicListenerException{
        if(topicListener == null){
            throw new NoTopicListenerException("No TopicListener is set");
        }
        String key = StringUtil.concat(patterns);
        ExecutorService es = topicServices.get(key);
        if(es == null){
            es = Executors.newSingleThreadExecutor();
            ExecutorService temp = topicServices.putIfAbsent(key, es);
            if(temp == null){
                SubscribePatternTask task = new SubscribePatternTask(topicListener, pool, patterns);
                es.execute(task);
                blockedTasks.putIfAbsent(key, task);
            }
        }
    }
    
    public void unsubscribe(String... topics) throws NoTopicListenerException{
        if(topicListener == null){
            throw new NoTopicListenerException("No TopicListener is set");
        }
        topicListener.unsubscribe(topics);
    }
    
    public void unsubscribePattern(String... patterns) throws NoTopicListenerException{
        if(topicListener == null){
            throw new NoTopicListenerException("No TopicListener is set");
        }
        topicListener.punsubscribe(patterns);
    }
    
    public long getSubsribersCount(String topic){
        Jedis jedis = pool.getResource();
        long count = jedis.publish(topic, Message.EMPTY);
        pool.returnResource(jedis);
        return count;
    }
    
    public void produce(String queue, String... messages){
        Jedis jedis = pool.getResource();
        for(String msg : messages){
            long count = jedis.incr(MQ.MSG_COUNT);
            String id = String.valueOf(count);
            String msgId = MQ.MSG_ID + id;
            Transaction tx = jedis.multi();
            tx.lpush(queue, id);
            tx.set(msgId, msg);
            tx.exec();
        }
        pool.returnResource(jedis);
    }
    
    public void produce(String queue, int expire, String... messages){
        Jedis jedis = pool.getResource();
        for(String msg : messages){
            long count = jedis.incr(MQ.MSG_COUNT);
            String id = String.valueOf(count);
            String msgId = MQ.MSG_ID + id;
            Transaction tx = jedis.multi();
            tx.lpush(queue, id);
            tx.set(msgId, msg);
            tx.expire(msgId, expire);
            tx.exec();
        }
        pool.returnResource(jedis);
    }
    
    public void produce(String queue, Date expireAt, String... messages){
        Jedis jedis = pool.getResource();
        for(String msg : messages){
            long count = jedis.incr(MQ.MSG_COUNT);
            String id = String.valueOf(count);
            String msgId = MQ.MSG_ID + id;
            Transaction tx = jedis.multi();
            tx.lpush(queue, id);
            tx.set(msgId, msg);
            tx.expireAt(msgId, expireAt.getTime());
            tx.exec();
        }
        pool.returnResource(jedis);
    }
    
    public void produceUrgency(String queue, String... messages){
        Jedis jedis = pool.getResource();
        for(String msg : messages){
            long count = jedis.incr(MQ.MSG_COUNT);
            String id = String.valueOf(count);
            String msgId = MQ.MSG_ID + id;
            Transaction tx = jedis.multi();
            tx.rpush(queue, id);
            tx.set(msgId, msg);
            tx.exec();
        }
        pool.returnResource(jedis);
    }
    
    public void produceUrgency(String queue, int expire, String... messages){
        Jedis jedis = pool.getResource();
        for(String msg : messages){
            long count = jedis.incr(MQ.MSG_COUNT);
            String id = String.valueOf(count);
            String msgId = MQ.MSG_ID + id;
            Transaction tx = jedis.multi();
            tx.rpush(queue, id);
            tx.set(msgId, msg);
            tx.expire(msgId, expire);
            tx.exec();
        }
        pool.returnResource(jedis);
    }
    
    public void produceUrgency(String queue, Date expireAt, String... messages){
        Jedis jedis = pool.getResource();
        for(String msg : messages){
            long count = jedis.incr(MQ.MSG_COUNT);
            String id = String.valueOf(count);
            String msgId = MQ.MSG_ID + id;
            Transaction tx = jedis.multi();
            tx.rpush(queue, id);
            tx.set(msgId, msg);
            tx.expireAt(msgId, expireAt.getTime());
            tx.exec();
        }
        pool.returnResource(jedis);
    }
    
    public void consume(QueueListener listener, String... queues){
        for(String queue : queues){
            ExecutorService es = queueServices.get(queue);
            if(es == null){
                es = Executors.newSingleThreadExecutor();
                ExecutorService temp = queueServices.putIfAbsent(queue, es);
                if(temp == null){
                    ConsumeQueueTask task = new ConsumeQueueTask(listener, pool, queue);
                    es.execute(task);
                    blockedTasks.putIfAbsent(queue, task);
                }
            }
        }
    }
    
    public void stopConsume(String... queues){
        for(String queue : queues){
            BlockedTask task = blockedTasks.get(queue);
            if(task != null){
                task.setStopped(true);
                task.getJedis().disconnect();
                blockedTasks.remove(queue);
            }
            ExecutorService es = queueServices.get(queue);
            if(es != null){
                es.shutdownNow();
                queueServices.remove(queue);
            }
        }
    }
    
    public void stopAllConsume(){
        Set<String> set = queueServices.keySet();
        for(String queue : set){
            stopConsume(queue);
        }
    }
    
    public void stopAllTopicTask(){
        Set<String> set = topicServices.keySet();
        for(String topic : set){
            BlockedTask task = blockedTasks.get(topic);
            if(task != null){
                task.getJedis().disconnect();
            }
            ExecutorService es = topicServices.get(topic);
            if(es != null){
                es.shutdownNow();
            }
        }
    }
    
    public void clearQueue(String... queues){
        Jedis jedis = pool.getResource();
        for(String queue : queues){
            long size = jedis.llen(queue);
            for(long i=0; i<size; i++){
                String id = jedis.rpop(queue);
                if(!StringUtil.isNull(id)){
                    jedis.del(MQ.MSG_ID + id);
                }
            }
        }
        pool.returnResource(jedis);
    }
    
    public void retainQueue(String queue, long retainSize){
        Jedis jedis = pool.getResource();
        long size = jedis.llen(queue);
        long count = size - retainSize;
        for(long i=0; i<count; i++){
            String id = jedis.rpop(queue);
            if(!StringUtil.isNull(id)){
                jedis.del(MQ.MSG_ID + id);
            }
        }
        pool.returnResource(jedis);
    }
    
    public long getQueueSize(String queue){
        Jedis jedis = pool.getResource();
        long size = jedis.llen(queue);
        pool.returnResource(jedis);
        return size;
    }

    public void setTopicListener(TopicListener topicListener) {
        this.topicListener = topicListener;
    }
    
    public void flushDB(){
        Jedis jedis = pool.getResource();
        jedis.flushDB();
        pool.returnResource(jedis);
    }

}
