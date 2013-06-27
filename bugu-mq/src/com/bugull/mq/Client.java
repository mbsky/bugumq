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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
    
    private final ConcurrentMap<String, ExecutorService> map = new ConcurrentHashMap<String, ExecutorService>();
    private final List<ExecutorService> list = new ArrayList<ExecutorService>();
    
    public Client(JedisPool pool){
        this.pool = pool;
    }
    
    public void publish(String topic, String message){
        Jedis jedis = pool.getResource();
        jedis.publish(topic, message);
        pool.returnResource(jedis);
    }
    
    public void subscribe(String... topics) throws NoTopicListenerException{
        if(topicListener == null){
            throw new NoTopicListenerException("No TopicListener is set");
        }
        ExecutorService es = Executors.newSingleThreadExecutor();
        SubscribeTopicTask task = new SubscribeTopicTask(topicListener, pool, topics);
        es.execute(task);
        list.add(es);
    }
    
    public void subscribePattern(String... patterns) throws NoTopicListenerException{
        if(topicListener == null){
            throw new NoTopicListenerException("No TopicListener is set");
        }
        ExecutorService es = Executors.newSingleThreadExecutor();
        SubscribePatternTask task = new SubscribePatternTask(topicListener, pool, patterns);
        es.execute(task);
        list.add(es);
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
        long count = jedis.publish(topic, MQ.EMPTY_MESSAGE);
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
            ExecutorService es = map.get(queue);
            if(es == null){
                es = Executors.newSingleThreadExecutor();
                ExecutorService temp = map.putIfAbsent(queue, es);
                if(temp == null){
                    ConsumeQueueTask task = new ConsumeQueueTask(listener, pool, queue);
                    es.execute(task);
                }
            }
        }
    }
    
    public void stopConsume(String... queues){
        for(String queue : queues){
            ExecutorService es = map.get(queue);
            if(es != null){
                es.shutdownNow();
                map.remove(queue);
            }
        }
    }
    
    public void stopAllConsume(){
        Set<String> set = map.keySet();
        for(String queue : set){
            stopConsume(queue);
        }
    }
    
    public void stopAllTopicTask(){
        for(ExecutorService es : list){
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
                String msgId = MQ.MSG_ID + id;
                jedis.del(msgId);
            }
        }
        pool.returnResource(jedis);
    }
    
    public void keepLatest(String queue, long n){
        Jedis jedis = pool.getResource();
        long size = jedis.llen(queue);
        long count = size - n;
        for(long i=0; i<count; i++){
            String id = jedis.rpop(queue);
            String msgId = MQ.MSG_ID + id;
            jedis.del(msgId);
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

}
