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

/**
 *
 * @author Frank Wen(xbwen@hotmail.com)
 */
public class FileMessage {
    
    private String fromClientId;
    private long fileId;
    private String messageType;
    private String filePath;
    private long fileLength;

    public long getFileId() {
        return fileId;
    }

    public void setFileId(long fileId) {
        this.fileId = fileId;
    }

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getFromClientId() {
        return fromClientId;
    }

    public void setFromClientId(String fromClientId) {
        this.fromClientId = fromClientId;
    }

    public long getFileLength() {
        return fileLength;
    }

    public void setFileLength(long fileLength) {
        this.fileLength = fileLength;
    }
    
    public static FileMessage parse(String s){
        String[] arr = s.split(MQ.SPLIT_MESSAGE);
        FileMessage fm = new FileMessage();
        fm.setFromClientId(arr[0]);
        fm.setFileId(Long.parseLong(arr[1]));
        fm.setMessageType(arr[2]);
        fm.setFilePath(arr[3]);
        fm.setFileLength(Long.parseLong(arr[4]));
        return fm;
    }
    
    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(fromClientId);
        sb.append(MQ.SPLIT_MESSAGE);
        sb.append(fileId);
        sb.append(MQ.SPLIT_MESSAGE);
        sb.append(messageType);
        sb.append(MQ.SPLIT_MESSAGE);
        sb.append(filePath);
        sb.append(MQ.SPLIT_MESSAGE);
        sb.append(fileLength);
        return sb.toString();
    }

}
