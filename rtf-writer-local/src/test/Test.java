/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jd.dw.rtf.writer.Testing;

import java.security.MessageDigest;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author anjinlong
 * @create 2017-07-05 9:50
 * @description description
 **/
public class Test {

  public static void main(String[] args) {
    String filenameWithPath = "hdfs://ns13/user/dd_edw_rtf/fdm_rtf.db/fdm_pek_orders_rtf_rcnt/.rtf.89100000-89199999-rtf-r-00891.rtf";

    String filename = filenameWithPath.substring(filenameWithPath.lastIndexOf("/")+1,
            filenameWithPath.length());

    System.out.println(filename);


//    long ts = 1528898189000l;
//    String mid = "200113713894";
//
//    ts = Long.parseLong(ts + mid.substring(mid.length() - 6));
//    System.out.println(ts);
//
//    long ts2 = 1528898189000l;
//    String mid2 = "200113713872";
//    ts2 = Long.parseLong(ts2 + mid2.substring(mid2.length() - 6));
//
//    System.out.println(ts2);
//
//    System.out.println(Tools.compareTS(String.valueOf(ts), String.valueOf(ts2)));


//    Random r = new Random();
//    for (int i = 1; i < 100; i++) {
//      System.out.println(r.nextInt(5));
//    }


//    String a ="a" + Constants.KEY_SPLIT_U0001+"b"+ Constants.KEY_SPLIT_U0001+"c"+Constants
// .KEY_SPLIT_U0001;
//    System.out.println(a.length());
//    System.out.println(Constants.KEY_SPLIT_U0001.length());
//    System.out.println(a.substring(0,a.length()-1).length());
//
//    String key = "A";
//    key = key.toLowerCase();
//    System.out.println("A : "+key);
//
//
//    LinkedList<Integer> l = new LinkedList<Integer>();
//    long start = System.currentTimeMillis();
//    for(int i=0 ;i< 1000000;i++){
//      l.add(i);
//    }
//    System.out.println("list add 用时："+(System.currentTimeMillis() - start));

  }


//  HashMap<String, StringBuffer> rtDataMap = new HashMap();
//
//  List<String> list = new ArrayList<String>();
//    list.add("1 v1\n");
//    list.add("2 v2\n");
//    list.add("3 v3\n");
//    list.add("1 --V1\n");
//
//
//    for (String l : list) {
//    String key = l.split(" ", -1)[0];
//    String v = l.split(" ", -1)[1];
//
//    if (rtDataMap.keySet().contains(key)) {
//      rtDataMap.get(key).append(v);
//    } else {
//      StringBuffer s = new StringBuffer();
//      s.append(v);
//      rtDataMap.put(key, s);
//    }
//  }
//
//    for (String key : rtDataMap.keySet()) {
//    System.out.print(key + " : " + rtDataMap.get(key));
//  }


//
//
//    System.out.println(ConsumerCommon.cleanData("NULL\tNULL\tNULL\n" +
//            "Time taken: 396.77 seconds, Fetched: 1 row(s)\n" +
//            "hive> "));
//
//    String a = "2835363785\u000110000007799\u00011255738";
//    System.out.println(a);
//    System.out.println(a.replace("\u0001",""));


//    String fields = "a,b,c";
//    StringBuffer value = new StringBuffer();
//
//
//    String[] fieldArray = fields.split(",", -1);
//    for (String field : fieldArray) {
//      value.append(field).append(Constants.SPLIT_TAB);
//    }
//
//
//    System.out.println(value.toString());
//    System.out.println(value.toString().length());
//
//    value = new StringBuffer();
//    for (int i = 0; i < fieldArray.length - 1; i++) {
//      value.append(fieldArray[i]).append(Constants.SPLIT_TAB);
//    }
//    value.append(fieldArray[fieldArray.length - 1]);
//    System.out.println(value.toString());
//    System.out.println(value.toString().length());
//
//
//    Properties properties = new Properties();
//    try {
//      properties.load(new FileInputStream("table.properties"));
//      String token = properties.getProperty("fdm_bd_waybill_waybill_m_chain.token");
//      System.out.println(token);
//    } catch (Exception e) {
//      Tools.printLog("load properties error:" + e.getMessage());
//      System.exit(1);
//    }


  public static String getMD5(String source) {
    String s = null;
    // 用来将字节转换成 16 进制表示的字符
    char hexDigits[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
            'e', 'f'};
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      md.update(source.getBytes());
      // MD5 的计算结果是一个 128 位的长整数，
      byte tmp[] = md.digest();

      // 用字节表示就是 16 个字节
      // 每个字节用 16 进制表示的话，使用两个字符，
      // 所以表示成 16 进制需要 32 个字符
      char str[] = new char[16 * 2];
      // 表示转换结果中对应的字符位置
      int k = 0;
      // 从第一个字节开始，对 MD5 的每一个字节
      // 转换成 16 进制字符的转换
      for (int i = 0; i < 16; i++) {
        // 取第 i 个字节
        byte byte0 = tmp[i];
        // 取字节中高 4 位的数字转换,>>> 为逻辑右移，将符号位一起右移
        str[k++] = hexDigits[byte0 >>> 4 & 0xf];
        // 取字节中低 4 位的数字转换
        str[k++] = hexDigits[byte0 & 0xf];
      }
      // 换后的结果转换为字符串
      s = new String(str);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return s;
  }

  static class MyTask implements Runnable {
    private ThreadPoolExecutor executor;
    private Map<Integer, String> filePartitionInfoMap;
    private int i;
    private CountDownLatch latch;

    public MyTask(ThreadPoolExecutor executor, Map<Integer, String> filePartitionInfoMap, int i,
                  CountDownLatch latch) {
      this.executor = executor;
      this.filePartitionInfoMap = filePartitionInfoMap;
      this.i = i;
      this.latch = latch;
    }

    public void run() {
      filePartitionInfoMap.put(i, "1111");

      System.out.println("正在执行task" + i + " " + filePartitionInfoMap.size() + ",getActiveCount:"
              + executor.getActiveCount() + ",getCorePoolSize:"
              + executor.getCorePoolSize());
      try {
        Thread.currentThread().sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      latch.countDown();
    }
  }


}


//    String a = "1/2/3";
//    System.out.println(a.substring(0, a.lastIndexOf("/") + 1));
//    System.out.println(a.substring(a.lastIndexOf("/") + 1, a.length()));
//
//    String filenameWithPath = "hdfs://ns6/user/dd_edw_test/fdm_test
// .db/fdm_pek_orders_chain_rtf/998000-998999-370867722";
//
//    String tmpFile = filenameWithPath.substring(0, filenameWithPath.lastIndexOf(Constants
// .FILE_PATH_SLASH) + 1)
//            + "_" + filenameWithPath.substring(filenameWithPath.lastIndexOf(Constants
// .FILE_PATH_SLASH) + 1, filenameWithPath.length());
//    System.out.println(tmpFile);


/**
 * String a = "sendpay      \n" +
 * "id           \n" +
 * "memberid     \n" +
 * "customername \n" +
 * "address      \n" +
 * "uprovince    \n" +
 * "ucity        \n" +
 * "ucounty      \n" +
 * "schoolid     \n" +
 * "schooluid    \n" +
 * "code         \n" +
 * "phone        \n" +
 * "usermob      \n" +
 * "email        \n" +
 * "payment      \n" +
 * "di           \n" +
 * "remark       \n" +
 * "createdate   \n" +
 * "state        \n" +
 * "state2       \n" +
 * "treateddate  \n" +
 * "treatedsum   \n" +
 * "treatedremark\n" +
 * "yn           \n" +
 * "totalprice   \n" +
 * "userremark   \n" +
 * "yun          \n" +
 * "usertruep    \n" +
 * "orderfid     \n" +
 * "orderfren    \n" +
 * "orderftime   \n" +
 * "orderftel    \n" +
 * "orderpuyn    \n" +
 * "paydate      \n" +
 * "outdate      \n" +
 * "paysuredate  \n" +
 * "resivedate   \n" +
 * "youhui       \n" +
 * "lastyu       \n" +
 * "newyu        \n" +
 * "trueyun      \n" +
 * "oprator      \n" +
 * "jifen        \n" +
 * "jyn          \n" +
 * "pyt          \n" +
 * "payremk      \n" +
 * "printtime    \n" +
 * "printx       \n" +
 * "premark      \n" +
 * "tidate       \n" +
 * "shdate       \n" +
 * "shdatesum    \n" +
 * "cky          \n" +
 * "fhy          \n" +
 * "chuna        \n" +
 * "ztdate       \n" +
 * "ztyn         \n" +
 * "fp           \n" +
 * "ziti         \n" +
 * "zioper       \n" +
 * "zititime     \n" +
 * "dabaotime    \n" +
 * "autoremark   \n" +
 * "operren      \n" +
 * "operq        \n" +
 * "kt           \n" +
 * "hzbj         \n" +
 * "cky2         \n" +
 * "cky3         \n" +
 * "printy       \n" +
 * "ordtype      \n" +
 * "bankname     \n" +
 * "userorderid  \n" +
 * "storeid      \n" +
 * "parentid     \n" +
 * "splittype    \n" +
 * "start_date   \n" +
 * "change_code  \n" +
 * "finish_date  \n" +
 * "townid       \n" +
 * "updatedate \n";
 * <p>
 * for (String t : a.split("\n")) {
 * //      System.out.println(".append(rtdataMap.get(\"" + t.trim() + "\")).append(Constants
 * .SPLIT_TAB)");
 * }
 **/