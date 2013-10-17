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
 * Constants for buguMQ framework.
 * 
 * @author Frank Wen(xbwen@hotmail.com)
 */
public final class MQ {
    
    //default value
    public static final int DEFAULT_PORT = 6379;
    public static final int DEFAULT_TIMEOUT = 3000;
    public static final int DEFAULT_DATABASE = 0;
    
    public static final int FILE_CHUNK_TIMEOUT = 30;  //30 seconds
    
    //for queue
    public static final String MSG_COUNT = "msg:count";
    
    public static final String MSG_ID = "msg:id:";
    
    //for topic
    public static final String RETAIN = "retain:";
    
    //for online
    public static final String ONLINE = "online:";
    
    //for file
    public static final String FILE_CLIENT = "file:client:";
    
    public static final String FILE_COUNT = "file:count";
    
    public static final String FILE_CHUNKS = "file:chunks:";
    
    //for message
    public static final String EMPTY_MESSAGE = "_E_";
    
    public static final String SPLIT_MESSAGE = "_#_";
    
    //file message type
    public static final int FILE_REQUEST = 1;
    public static final int FILE_ACCEPT = 2;
    public static final int FILE_REJECT = 3;

}
