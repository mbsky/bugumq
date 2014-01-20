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

package com.bugull.mq.message;

import com.bugull.mq.utils.MQ;
import com.bugull.mq.utils.BinaryUtil;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Message used for boradcast files.
 * 
 * @author Frank Wen(xbwen@hotmail.com)
 */
public class FileBroadcastMessage {
    
    private byte type;
    private long fileId;
    private Map<String, String> extras;
    private byte[] fileData;

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public long getFileId() {
        return fileId;
    }

    public void setFileId(long fileId) {
        this.fileId = fileId;
    }

    public Map<String, String> getExtras() {
        return extras;
    }

    public void setExtras(Map<String, String> extras) {
        this.extras = extras;
    }

    public byte[] getFileData() {
        return fileData;
    }

    public void setFileData(byte[] fileData) {
        this.fileData = fileData;
    }
    
    public static FileBroadcastMessage parse(byte[] message){
        FileBroadcastMessage fbm = new FileBroadcastMessage();
        byte theType = message[0];
        fbm.setType(theType);
        long theFileId = BinaryUtil.toLong(Arrays.copyOfRange(message, 1, 9));
        fbm.setFileId(theFileId);
        int len = message.length;
        if(len>9){
            byte[] exData = Arrays.copyOfRange(message, 9, len);
            if(theType==MQ.BROADCAST_DATA){
                fbm.setFileData(exData);
            }
            else if(theType==MQ.BROADCAST_START){
                Map<String, String> map = new HashMap<String, String>();
                String exStr = null;
                try{
                    exStr = new String(exData, MQ.CHARSET);
                }catch(UnsupportedEncodingException ex){

                }
                String[] arr = exStr.split(MQ.SPLIT_MESSAGE);
                for(String s : arr){
                    String[] kv = s.split(MQ.SPLIT_EXTRA);
                    map.put(kv[0], kv[1]);
                }
                fbm.setExtras(map);
            }
        } 
        return fbm;
    }
    
    public byte[] toBytes(){
        byte[] fileIdData = BinaryUtil.fromLong(fileId);
        byte[] exData = null;
        if(type==MQ.BROADCAST_DATA){
            exData = fileData;
        }
        else if(type==MQ.BROADCAST_START && extras!=null){
            StringBuilder sb = new StringBuilder();
            Set<Map.Entry<String, String>> set = extras.entrySet();
            for(Map.Entry<String, String> entry : set){
                sb.append(entry.getKey()).append(MQ.SPLIT_EXTRA).append(entry.getValue());
                sb.append(MQ.SPLIT_MESSAGE);
            }
            String s = sb.toString();
            String data = s.substring(0, s.length() - MQ.SPLIT_MESSAGE.length());
            try{
                exData = data.getBytes(MQ.CHARSET);
            }catch(UnsupportedEncodingException ex){
                
            }
        }
        byte[] result = null;
        if(exData == null){
            result = new byte[9];
        }else{
            int len = 9 + exData.length;
            result = new byte[len];
        }
        result[0] = type;
        System.arraycopy(fileIdData, 0, result, 1, 8);
        if(exData != null){
            System.arraycopy(exData, 0, result, 9, exData.length);
        }
        return result;
    }

}
