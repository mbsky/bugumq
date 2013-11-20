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
 * Thread to get file chunks data from redis.
 * 
 * @author Frank Wen(xbwen@hotmail.com)
 */
public class GetFileDataTask implements Runnable {
    
    private FileListener fileListener;
    private JedisPool pool;
    private long fileId;

    public GetFileDataTask(FileListener fileListener, JedisPool pool, long fileId) {
        this.fileListener = fileListener;
        this.pool = pool;
        this.fileId = fileId;
    }

    @Override
    public void run() {
        boolean stopped = false;
        byte[] queue = (MQ.FILE_CHUNKS + fileId).getBytes();
        while(!stopped){
            Jedis jedis = null;
            try{
                jedis = pool.getResource();
                List<byte[]> list = jedis.brpop(MQ.FILE_CHUNK_TIMEOUT, queue);
                synchronized(fileListener){
                    if(list!=null && list.size()==2){
                        byte[] data = list.get(1);
                        if(data.length != MQ.EMPTY_MESSAGE.length()){
                            fileListener.onFileData(fileId, data);
                        }
                        else{
                            String eof = new String(data);
                            if(eof.equals(MQ.EMPTY_MESSAGE)){
                                stopped = true;
                                fileListener.onFileEnd(fileId);
                            }else{
                                fileListener.onFileData(fileId, data);
                            }
                        }
                    }
                    else{
                        stopped = true;
                        fileListener.onError(fileId);
                    }
                }
            }catch(Exception ex){
                stopped = true;
                synchronized(fileListener){
                    fileListener.onError(fileId);
                }
            }finally{
                JedisUtil.returnToPool(pool, jedis);
            }
        }
    }

}
