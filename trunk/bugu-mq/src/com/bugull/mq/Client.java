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

import com.bugull.mq.exception.MQException;
import com.bugull.mq.message.FileMessage;
import com.bugull.mq.listener.TopicListener;
import com.bugull.mq.listener.QueueListener;
import com.bugull.mq.listener.FileListener;
import com.bugull.mq.listener.FileClientListener;
import com.bugull.mq.listener.FileBroadcastListener;
import com.bugull.mq.message.FileBroadcastMessage;
import com.bugull.mq.task.SubscribeTopicTask;
import com.bugull.mq.task.SubscribePatternTask;
import com.bugull.mq.task.SubscribeFileBroadcastTask;
import com.bugull.mq.task.GetFileDataTask;
import com.bugull.mq.task.ConsumeQueueTask;
import com.bugull.mq.task.BlockedTask;
import com.bugull.mq.utils.StringUtil;
import com.bugull.mq.utils.JedisUtil;
import com.bugull.mq.utils.ByteUtil;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

/**
 * Presents an MQ client. All MQ operation is implemented here.
 * 
 * @author Frank Wen(xbwen@hotmail.com)
 */
public class Client {
    
    private JedisPool pool;
    
    private TopicListener topicListener;
    private FileListener fileListener;
    private FileBroadcastListener broadcastListener;
    
    private final ConcurrentMap<String, ExecutorService> queueServices = new ConcurrentHashMap<String, ExecutorService>();
    private final ConcurrentMap<String, ExecutorService> topicServices = new ConcurrentHashMap<String, ExecutorService>();
    private final ConcurrentMap<String, ExecutorService> broadcastServices = new ConcurrentHashMap<String, ExecutorService>();
    
    //store the blocked tasks, in order to stop it and close the jedis client.
    private final ConcurrentMap<String, BlockedTask> blockedTasks = new ConcurrentHashMap<String, BlockedTask>();
    
    public Client(JedisPool pool){
        this.pool = pool;
    }
    
    public void publish(String topic, String message) throws MQException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            jedis.publish(topic, message);
        }catch(Exception ex){
            //Note: catch Exception here, because there are many runtime exception in Jedis.
            //Following code is same like this.
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void publishRetain(String topic, String message) throws MQException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            Transaction tx = jedis.multi();
            tx.publish(topic, message);
            tx.set(MQ.RETAIN + topic, message);
            tx.exec();
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void clearRetainMessage(String... topics) throws MQException {
        int len = topics.length;
        String[] keys = new String[len];
        for(int i=0; i< len; i++){
            keys[i] = MQ.RETAIN + topics[i];
        }
        Jedis jedis = null;
        try{
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
                SubscribeTopicTask task = new SubscribeTopicTask(topicListener, pool, topics);
                es.execute(task);
                blockedTasks.putIfAbsent(key, task);
            }
        }
        for(String topic : topics){
            topicListener.addTimer(topic, true);
        }
    }
    
    public void subscribePattern(String... patterns) {
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
        for(String pattern : patterns){
            topicListener.addTimer(pattern, false);
        }
    }
    
    public void unsubscribe(String... topics) throws MQException {
        try{
            topicListener.unsubscribe(topics);
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }
        for(String topic : topics){
            topicListener.removeTimer(topic);
        }
    }
    
    public void unsubscribePattern(String... patterns) throws MQException {
        try{
            topicListener.punsubscribe(patterns);
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }
        for(String pattern : patterns){
            topicListener.removeTimer(pattern);
        }
    }
    
    public void produce(String queue, String... messages) throws MQException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            for(String msg : messages){
                long count = jedis.incr(MQ.MSG_COUNT);
                String id = String.valueOf(count);
                String msgId = MQ.MSG_ID + id;
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
    
    public void produce(String queue, int expire, String... messages) throws MQException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
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
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void produce(String queue, Date expireAt, String... messages) throws MQException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
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
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void produceUrgency(String queue, String... messages) throws MQException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            for(String msg : messages){
                long count = jedis.incr(MQ.MSG_COUNT);
                String id = String.valueOf(count);
                String msgId = MQ.MSG_ID + id;
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
    
    public void produceUrgency(String queue, int expire, String... messages) throws MQException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
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
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void produceUrgency(String queue, Date expireAt, String... messages) throws MQException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
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
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
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
                try{
                    task.getJedis().disconnect();
                }catch(Exception ex){
                    //ignore ex
                }
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
                task.setStopped(true);
                try{
                    task.getJedis().disconnect();
                }catch(Exception ex){
                    //ignore ex
                }
                blockedTasks.remove(topic);
            }
            ExecutorService es = topicServices.get(topic);
            if(es != null){
                es.shutdownNow();
                topicServices.remove(topic);
            }
        }
    }
    
    public void stopAllFileBroadcastTask(){
        Set<String> set = broadcastServices.keySet();
        for(String topic : set){
            BlockedTask task = blockedTasks.get(topic);
            if(task != null){
                task.setStopped(true);
                try{
                    task.getJedis().disconnect();
                }catch(Exception ex){
                    //ignore ex
                }
                blockedTasks.remove(topic);
            }
            ExecutorService es = broadcastServices.get(topic);
            if(es != null){
                es.shutdownNow();
                broadcastServices.remove(topic);
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
                        jedis.del(MQ.MSG_ID + id);
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
                    jedis.del(MQ.MSG_ID + id);
                }
            }
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public long getQueueSize(String queue) throws MQException {
        long size = 0;
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            size = jedis.llen(queue);
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
        return size;
    }
    
    public boolean isOnline(String clientId) throws MQException {
        boolean result = false;
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            result = jedis.exists(MQ.ONLINE + clientId);
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
        return result;
    }
    
    public List<Boolean> isOnline(List<String> clientList) throws MQException {
        List<Boolean> results = new ArrayList<Boolean>();
        List<Response<Boolean>> responseList = new ArrayList<Response<Boolean>>();
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            responseList = new ArrayList<Response<Boolean>>();
            Pipeline p = jedis.pipelined();
            for(String clientId : clientList){
                responseList.add(p.exists(MQ.ONLINE + clientId));
            }
            p.sync();
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
        for(Response<Boolean> response : responseList){
            results.add(response.get());
        }
        return results;
    }
    
    public void clearAll() throws MQException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            jedis.flushDB();
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }

    public void setTopicListener(TopicListener topicListener) {
        this.topicListener = topicListener;
    }
    
    public void setFileListener(FileListener fileListener){
        this.fileListener = fileListener;
        this.consume(new FileClientListener(fileListener), MQ.FILE_CLIENT + Connection.getInstance().getClientId());
    }
    
    public long requestSendFile(String toClientId, Map<String, String> extras) throws MQException {
        Jedis jedis = null;
        long fileId = 0;
        try{
            jedis = pool.getResource();
            fileId = jedis.incr(MQ.FILE_COUNT);
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
        //if fileId==0, exception is catched
        if(fileId > 0){
            FileMessage fm = new FileMessage();
            fm.setFromClientId(Connection.getInstance().getClientId());
            fm.setType(MQ.FILE_REQUEST);
            fm.setFileId(fileId);
            fm.setExtras(extras);
            this.produce(MQ.FILE_CLIENT + toClientId, MQ.FILE_MSG_TIMEOUT, fm.toString());
        }
        return fileId;
    }
    
    public void sendFileData(long fileId, byte[] data) throws MQException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            byte[] queue = (MQ.FILE_CHUNKS + fileId).getBytes();
            jedis.lpush(queue, data);
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void sendEndOfFile(long fileId) throws MQException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            byte[] queue = (MQ.FILE_CHUNKS + fileId).getBytes();
            jedis.lpush(queue, MQ.EOF_MESSAGE.getBytes());
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void acceptReceiveFile(String toClientId, long fileId, Map<String, String> extras) throws MQException {
        //send accept message
        FileMessage fm = new FileMessage();
        fm.setFromClientId(Connection.getInstance().getClientId());
        fm.setType(MQ.FILE_ACCEPT);
        fm.setFileId(fileId);
        fm.setExtras(extras);
        this.produce(MQ.FILE_CLIENT + toClientId, MQ.FILE_MSG_TIMEOUT, fm.toString());

        //start a thread to receive file data
        GetFileDataTask task = new GetFileDataTask(fileListener, pool, fileId);
        new Thread(task).start();
    }
    
    public void rejectReceiveFile(String toClientId, long fileId, Map<String, String> extras) throws MQException {
        //send reject message;
        FileMessage fm = new FileMessage();
        fm.setFromClientId(Connection.getInstance().getClientId());
        fm.setType(MQ.FILE_REJECT);
        fm.setFileId(fileId);
        fm.setExtras(extras);
        this.produce(MQ.FILE_CLIENT + toClientId, MQ.FILE_MSG_TIMEOUT, fm.toString());
    }
    
    public void setFileBroadcastListener(FileBroadcastListener broadcastListener){
        this.broadcastListener = broadcastListener;
    }
    
    public void subscribeFileBroadcast(String... topics) {
        String key = StringUtil.concat(topics);
        ExecutorService es = broadcastServices.get(key);
        if(es == null){
            es = Executors.newSingleThreadExecutor();
            ExecutorService temp = broadcastServices.putIfAbsent(key, es);
            if(temp == null){
                SubscribeFileBroadcastTask task = new SubscribeFileBroadcastTask(broadcastListener, pool, ByteUtil.getTopicsBytes(topics));
                es.execute(task);
                blockedTasks.putIfAbsent(key, task);
            }
        }
        for(String topic : topics){
            broadcastListener.addTimer(topic);
        }
    }
    
    public void unsubscribeFileBroadcast(String... topics) throws MQException {
        try{
            broadcastListener.unsubscribe(ByteUtil.getTopicsBytes(topics));
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }
        for(String topic : topics){
            broadcastListener.removeTimer(topic);
        }
    }
    
    public long startBroadcastFile(String topic, Map<String, String> extras) throws MQException {
        Jedis jedis = null;
        long fileId = 0;
        try{
            jedis = pool.getResource();
            fileId = jedis.incr(MQ.BROADCAST_COUNT);
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
        //if fileId==0, exception is catched
        if(fileId > 0){
            FileBroadcastMessage fbm = new FileBroadcastMessage();
            fbm.setType(MQ.BROADCAST_START);
            fbm.setFileId(fileId);
            fbm.setExtras(extras);
            byte[] message = fbm.toBytes();
            this.publish(topic, message);
        }
        return fileId;
    }
    
    public void endBroadcastFile(String topic, long fileId) throws MQException {
        FileBroadcastMessage fbm = new FileBroadcastMessage();
        fbm.setType(MQ.BROADCAST_END);
        fbm.setFileId(fileId);
        byte[] message = fbm.toBytes();
        this.publish(topic, message);
    }
    
    public void broadcastFileData(String topic, long fileId, byte[] data) throws MQException {
        FileBroadcastMessage fbm = new FileBroadcastMessage();
        fbm.setType(MQ.BROADCAST_DATA);
        fbm.setFileId(fileId);
        fbm.setFileData(data);
        byte[] message = fbm.toBytes();
        this.publish(topic, message);
    }
    
    private void publish(String topic, byte[] message) throws MQException {
        byte[] channel = null;
        try{
            channel = topic.getBytes(MQ.CHARSET);
        } catch (UnsupportedEncodingException ex) {
            
        }
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            jedis.publish(channel, message);
        }catch(Exception ex){
            throw new MQException(ex.getMessage());
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }

}
