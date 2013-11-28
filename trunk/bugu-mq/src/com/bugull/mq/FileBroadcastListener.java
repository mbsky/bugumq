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

import java.util.Arrays;

/**
 *
 * @author Frank Wen(xbwen@hotmail.com)
 */
public abstract class FileBroadcastListener extends BinaryTopicListener {
    
    public abstract void onFileStart(long fileId);
    
    public abstract void onFileEnd(long fileId);
    
    public abstract void onFileData(long fileId, byte[] data);

    @Override
    public void onBinaryMessage(String topic, byte[] message) {
        if(!topic.equals(MQ.FILE_BROADCAST) || message.length <9){
            return;
        }
        long fileId = ByteUtil.toLong(Arrays.copyOf(message, 8));
        byte type = message[8];
        if(type==MQ.BROADCAST_START){
            onFileStart(fileId);
        }
        else if(type==MQ.BROADCAST_END){
            onFileEnd(fileId);
        }
        else if(type==MQ.BROADCAST_DATA){
            onFileData(fileId, Arrays.copyOfRange(message, 9, message.length));
        }
    }

}
