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

package com.bugull.mq.listener;

import com.bugull.mq.message.FileBroadcastMessage;
import com.bugull.mq.MQ;
import com.bugull.mq.listener.BinaryTopicListener;
import java.util.Map;

/**
 *
 * @author Frank Wen(xbwen@hotmail.com)
 */
public abstract class FileBroadcastListener extends BinaryTopicListener {
    
    public abstract void onFileStart(String topic, long fileId, Map<String,String> extras);
    
    public abstract void onFileEnd(String topic, long fileId);
    
    public abstract void onFileData(String topic, long fileId, byte[] data);

    @Override
    public void onBinaryMessage(String topic, byte[] message) {
        if(message.length <9){
            return;
        }
        FileBroadcastMessage fbm = FileBroadcastMessage.parse(message);
        byte type = fbm.getType();
        long fileId = fbm.getFileId();
        if(type==MQ.BROADCAST_START){
            onFileStart(topic, fileId, fbm.getExtras());
        }
        else if(type==MQ.BROADCAST_END){
            onFileEnd(topic, fileId);
        }
        else if(type==MQ.BROADCAST_DATA){
            onFileData(topic, fileId, fbm.getFileData());
        }
    }

}
