package org.apache.ignite.util.model;/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Random;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/** */
public class TheEpicData implements Serializable {
    /** */
    private long long1;
    /** */
    @QuerySqlField()
    private long long2;
    /** */
    @QuerySqlField(index = true)
    private long long3;
    /** */
    private byte byte1;
    /** */
    @QuerySqlField()
    private byte byte2;
    /** */
    private int id;
    /** */
    @QuerySqlField(index = true)
    private int int2;
    /** */
    @QuerySqlField()
    private int int3;
    /** */
    private String str1 = "adfgsaf sas53c";
    /** */
    @QuerySqlField(index = true)
    private String indexedStr = "abc657sdfgtgserygertjdrjfgj dfg53";
    /** */
    @QuerySqlField()
    private String str3 = "abc657dfzhgdsfsgsdgsdfgsdfxc vjljoljp[i[ipoi[pasfojasdofiujoip3u5oijoijopzkjgbsg";
    /** */
    private byte[] raw1 = new byte[60];
    /** */
    private byte[] raw2 = new byte[771];
    /** */
    private byte[] raw3 = new byte[17_455];
    /** */
    private byte[] raw4 = new byte[35_724];

    /** */
    public TheEpicData(int id, boolean randomize) {
        this.id = id;

        if (randomize) {
            Random rnd = new Random();

            raw1 = rndBytes(raw1.length, rnd);
            raw2 = rndBytes(raw2.length, rnd);
            raw3 = rndBytes(raw3.length, rnd);
            raw4 = rndBytes(raw4.length, rnd);

            indexedStr = rndStr(indexedStr.length(), rnd);
            str3 = rndStr(str3.length(), rnd);
        }
    }

    public long getLong1() {
        return long1;
    }

    public void setLong1(long long1) {
        this.long1 = long1;
    }

    public long getLong2() {
        return long2;
    }

    public void setLong2(long long2) {
        this.long2 = long2;
    }

    public long getLong3() {
        return long3;
    }

    public void setLong3(long long3) {
        this.long3 = long3;
    }

    public byte getByte1() {
        return byte1;
    }

    public void setByte1(byte byte1) {
        this.byte1 = byte1;
    }

    public byte getByte2() {
        return byte2;
    }

    public void setByte2(byte byte2) {
        this.byte2 = byte2;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getInt2() {
        return int2;
    }

    public void setInt2(int int2) {
        this.int2 = int2;
    }

    public int getInt3() {
        return int3;
    }

    public void setInt3(int int3) {
        this.int3 = int3;
    }

    public String getStr1() {
        return str1;
    }

    public void setStr1(String str1) {
        this.str1 = str1;
    }

    public String getIndexedStr() {
        return indexedStr;
    }

    public void setIndexedStr(String indexedStr) {
        this.indexedStr = indexedStr;
    }

    public String getStr3() {
        return str3;
    }

    public void setStr3(String str3) {
        this.str3 = str3;
    }

    public byte[] getRaw1() {
        return raw1;
    }

    public void setRaw1(byte[] raw1) {
        this.raw1 = raw1;
    }

    public byte[] getRaw2() {
        return raw2;
    }

    public void setRaw2(byte[] raw2) {
        this.raw2 = raw2;
    }

    public byte[] getRaw3() {
        return raw3;
    }

    public void setRaw3(byte[] raw3) {
        this.raw3 = raw3;
    }

    public byte[] getRaw4() {
        return raw4;
    }

    public void setRaw4(byte[] raw4) {
        this.raw4 = raw4;
    }

    /** */
    public int dataSize() throws IllegalAccessException {
        int res = 0;

        for (Field f : TheEpicData.class.getDeclaredFields()) {
            if (f.getType() == int.class || f.getType() == float.class)
                res += 4;
            else if (f.getType() == long.class || f.getType() == double.class)
                res += 8;
            else if (f.getType() == short.class)
                res += 2;
            else if (f.getType() == byte.class)
                res += 2;
            else if (f.getType() == byte[].class)
                res += 4 + ((byte[])f.get(this)).length;
            else if (f.getType() == String.class)
                res += 4 + ((String)f.get(this)).getBytes().length;
            else
                throw new RuntimeException("Unknown field type: " + f.getType().getName());
        }

        return res;
    }

    /** */
    private static byte[] rndBytes(int maxLen, Random rnd) {
        if (rnd == null)
            rnd = new Random();

        byte[] res = new byte[maxLen / 2 + rnd.nextInt(maxLen / 2)];

        rnd.nextBytes(res);

        return res;
    }

    /** */
    private static String rndStr(int maxLen, Random rnd) {
        if (rnd == null)
            rnd = new Random();

        return rnd.ints(48, 122)
            .filter(i -> (i < 57 || i > 65) && (i < 90 || i > 97))
            .mapToObj(i -> (char)i)
            .limit(maxLen / 2 + rnd.nextInt(maxLen / 2))
            .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append)
            .toString();
    }
}