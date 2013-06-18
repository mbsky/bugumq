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
    
    private final ConcurrentMap<String, ExecutorService> map = new ConcurrentHashMap<String, ExecutorService>();
    
    public Client(JedisPool pool){
        this.pool = pool;
    }
    
    public void publish(String topic, String message){
        Jedis jedis = pool.getResource();
        jedis.publish(topic, message);
        pool.returnResource(jedis);
    }
    
    public void subscribeTopic(TopicMessageListener listener, String... topics){
        Jedis jedis = pool.getResource();
        jedis.subscribe(listener, topics);
        pool.returnResource(jedis);
    }
    
    public void subscribePattern(PatternMessageListener listener, String... patterns){
        Jedis jedis = pool.getResource();
        jedis.psubscribe(listener, patterns);
        pool.returnResource(jedis);
    }
    
    public void unsubscribeTopic(String... topics){
        new Unsubscriber().unsubscribe(topics);
    }
    
    public void unsubscribeAllTopic(){
        new Unsubscriber().unsubscribe();
    }
    
    public void unsubscribePattern(String... patterns){
        new Unsubscriber().punsubscribe(patterns);
    }
    
    public void unsubscribeAllPattern(){
        new Unsubscriber().punsubscribe();
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
            long id = jedis.incr(MQ.MSG_COUNT);
            String msgId = MQ.MSG_ID + id;
            Transaction tx = jedis.multi();
            tx.lpush(queue, msgId);
            tx.set(msgId, msg);
            tx.exec();
        }
        pool.returnResource(jedis);
    }
    
    public void produce(String queue, long ttl, String... messages){
        Jedis jedis = pool.getResource();
        for(String msg : messages){
            long id = jedis.incr(MQ.MSG_COUNT);
            String msgId = MQ.MSG_ID + id;
            Transaction tx = jedis.multi();
            tx.lpush(queue, msgId);
            tx.set(msgId, msg);
            tx.ttl(msgId);
            tx.exec();
        }
        pool.returnResource(jedis);
    }
    
    public void produce(String queue, Date expire, String... messages){
        Jedis jedis = pool.getResource();
        for(String msg : messages){
            long id = jedis.incr(MQ.MSG_COUNT);
            String msgId = MQ.MSG_ID + id;
            Transaction tx = jedis.multi();
            tx.lpush(queue, msgId);
            tx.set(msgId, msg);
            tx.expireAt(msgId, expire.getTime());
            tx.exec();
        }
        pool.returnResource(jedis);
    }
    
    public void produceEmergency(String queue, String... messages){
        Jedis jedis = pool.getResource();
        for(String msg : messages){
            long id = jedis.incr(MQ.MSG_COUNT);
            String msgId = MQ.MSG_ID + id;
            Transaction tx = jedis.multi();
            tx.rpush(queue, msgId);
            tx.set(msgId, msg);
            tx.exec();
        }
        pool.returnResource(jedis);
    }
    
    public void produceEmergency(String queue, long ttl, String... messages){
        Jedis jedis = pool.getResource();
        for(String msg : messages){
            long id = jedis.incr(MQ.MSG_COUNT);
            String msgId = MQ.MSG_ID + id;
            Transaction tx = jedis.multi();
            tx.rpush(queue, msgId);
            tx.set(msgId, msg);
            tx.ttl(msgId);
            tx.exec();
        }
        pool.returnResource(jedis);
    }
    
    public void produceEmergency(String queue, Date expire, String... messages){
        Jedis jedis = pool.getResource();
        for(String msg : messages){
            long id = jedis.incr(MQ.MSG_COUNT);
            String msgId = MQ.MSG_ID + id;
            Transaction tx = jedis.multi();
            tx.rpush(queue, msgId);
            tx.set(msgId, msg);
            tx.expireAt(msgId, expire.getTime());
            tx.exec();
        }
        pool.returnResource(jedis);
    }
    
    public void consume(QueueMessageListener listener, String... queues){
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
    
    public void clearQueue(String... queues){
        Jedis jedis = pool.getResource();
        jedis.del(queues);
        pool.returnResource(jedis);
    }
    
    public long getQueueSize(String queue){
        Jedis jedis = pool.getResource();
        long size = jedis.llen(queue);
        pool.returnResource(jedis);
        return size;
    }

}
