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

import com.bugull.mq.Connection;
import com.bugull.mq.utils.MQ;
import com.bugull.mq.exception.MQException;
import com.bugull.mq.listener.FileBroadcastListener;
import com.bugull.mq.listener.FileClientListener;
import com.bugull.mq.listener.FileListener;
import com.bugull.mq.message.FileBroadcastMessage;
import com.bugull.mq.message.FileMessage;
import com.bugull.mq.task.GetFileDataTask;
import com.bugull.mq.task.SubscribeBinaryTopicTask;
import com.bugull.mq.utils.BinaryUtil;
import com.bugull.mq.utils.JedisUtil;
import com.bugull.mq.utils.StringUtil;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 *
 * @author Frank Wen(xbwen@hotmail.com)
 */
public class FileClient extends AbstractClient {
    
    private FileListener fileListener;
    private FileBroadcastListener broadcastListener;
    
    public FileClient(JedisPool pool){
        this.pool = pool;
    }
    
    public void setFileListener(FileListener fileListener){
        this.fileListener = fileListener;
        Client client = Connection.getInstance().getClient();
        client.consume(new FileClientListener(fileListener), MQ.FILE_CLIENT + Connection.getInstance().getClientId());
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
            Client client = Connection.getInstance().getClient();
            client.produce(MQ.FILE_CLIENT + toClientId, MQ.FILE_MSG_TIMEOUT, fm.toString());
        }
        return fileId;
    }
    
    public void sendFileData(long fileId, byte[] data) throws MQException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            byte[] queue = (MQ.FILE_CHUNKS + fileId).getBytes(MQ.CHARSET);
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
            byte[] queue = (MQ.FILE_CHUNKS + fileId).getBytes(MQ.CHARSET);
            jedis.lpush(queue, MQ.EOF_MESSAGE.getBytes(MQ.CHARSET));
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
        Client client = Connection.getInstance().getClient();
        client.produce(MQ.FILE_CLIENT + toClientId, MQ.FILE_MSG_TIMEOUT, fm.toString());

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
        Client client = Connection.getInstance().getClient();
        client.produce(MQ.FILE_CLIENT + toClientId, MQ.FILE_MSG_TIMEOUT, fm.toString());
    }
    
    public void setFileBroadcastListener(FileBroadcastListener broadcastListener){
        this.broadcastListener = broadcastListener;
    }
    
    public void subscribeFileBroadcast(String... topics) {
        String key = StringUtil.concat(topics);
        ExecutorService es = topicServices.get(key);
        if(es == null){
            es = Executors.newSingleThreadExecutor();
            ExecutorService temp = topicServices.putIfAbsent(key, es);
            if(temp == null){
                SubscribeBinaryTopicTask task = new SubscribeBinaryTopicTask(broadcastListener, pool, BinaryUtil.toBytes(topics));
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
            broadcastListener.unsubscribe(BinaryUtil.toBytes(topics));
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
            BinaryClient binaryClient = Connection.getInstance().getBinaryClient();
            binaryClient.publish(topic, message);
        }
        return fileId;
    }
    
    public void endBroadcastFile(String topic, long fileId) throws MQException {
        FileBroadcastMessage fbm = new FileBroadcastMessage();
        fbm.setType(MQ.BROADCAST_END);
        fbm.setFileId(fileId);
        byte[] message = fbm.toBytes();
        BinaryClient binaryClient = Connection.getInstance().getBinaryClient();
        binaryClient.publish(topic, message);
    }
    
    public void broadcastFileData(String topic, long fileId, byte[] data) throws MQException {
        FileBroadcastMessage fbm = new FileBroadcastMessage();
        fbm.setType(MQ.BROADCAST_DATA);
        fbm.setFileId(fileId);
        fbm.setFileData(data);
        byte[] message = fbm.toBytes();
        BinaryClient binaryClient = Connection.getInstance().getBinaryClient();
        binaryClient.publish(topic, message);
    }
    
    public void stopAllFileBroadcastTask(){
        super.stopAllTopicTask();
        if(broadcastListener != null){
            broadcastListener.closeAllTimer();
        }
    }

}
