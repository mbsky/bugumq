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

package com.bugull.mq.utils;

import java.io.UnsupportedEncodingException;

/**
 *
 * @author Frank Wen(xbwen@hotmail.com)
 */
public class ByteUtil {
    
    public static byte[] fromLong(long x){
        byte[] result = new byte[8];
        result[0] = (byte) ((x >> 56) & 0xFF);
        result[1] = (byte) ((x >> 48) & 0xFF);
        result[2] = (byte) ((x >> 40) & 0xFF);
        result[3] = (byte) ((x >> 32) & 0xFF);
        result[4] = (byte) ((x >> 24) & 0xFF);
        result[5] = (byte) ((x >> 16) & 0xFF);
        result[6] = (byte) ((x >> 8) & 0xFF);
        result[7] = (byte) (x & 0xFF);
        return result;
    }
    
    public static long toLong(byte[] bytes){
        long value = 0;
        for (int i=0; i<8; i++) {
            int shift = (8 - 1 - i) * 8;
            value += ( (long)(bytes[i] & 0xFF) ) << shift;
        }
        return value;
    }
    
    public static byte[][] getTopicsBytes(String... topics){
        int len = topics.length;
        byte[][] channel = new byte[len][];
        for(int i=0; i<len; i++){
            try {
                channel[i] = topics[i].getBytes("UTF-8");
            } catch (UnsupportedEncodingException ex) {
                
            }
        }
        return channel;
    }

}
