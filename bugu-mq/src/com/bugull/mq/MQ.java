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
 * Constants for the MQ.
 * 
 * @author Frank Wen(xbwen@hotmail.com)
 */
public final class MQ {
    
    public static final int DEFAULT_PORT = 6379;
    public static final int DEFAULT_SO_TIMEOUT = 2000;
    public static final int DEFAULT_DATABASE = 0;
    
    public static final String EMPTY_MESSAGE = "_EMPTY_";
    
    public static final String MSG_COUNT = "msg:count";
    
    public static final String MSG_ID = "msg:id:";

}
