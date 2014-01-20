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

/**
 *
 * @author Frank Wen(xbwen@hotmail.com)
 */
public final class StringUtil {
    
    /**
     * is empty string or not
     * @param s
     * @return 
     */
    public static boolean isEmpty(String s){
        return s==null || s.trim().length() == 0;
    }
    
    /**
     * Concat string array elements with comma
     * @param arr
     * @return 
     */
    public static String concat(String... arr){
        StringBuilder sb = new StringBuilder();
        for(String s : arr){
            sb.append(s).append(",");
        }
        String result = sb.toString();
        return result.substring(0, result.length()-1);
    }

}
