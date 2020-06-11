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

/**
 * @author anjinlong
 * @create 2018-01-12 17:58
 * @description description
 **/
public class TestKey {


  public static void main(String args[]) {
    String jdq = "70413947769 - {\"mid\": 2890860597, \"db\": \"jdorders_27\", \"sch\": " +
            "\"jdorders_27\", \"tab\": \"orders_889\", \"opt\": \"UPDATE\", \"ts\": " +
            "1515686403000, \"ddl\": null, \"err\": null, \"src\": {\"remark\": \"\", " +
            "\"orderfid\": null, \"autoremark\": null, \"ordtype\": null, \"paysuredate\": null, " +
            "\"parentid\": \"70413947481\", \"hzbj\": null, \"Uprovince\": \"18\", \"shdatesum\":" +
            " \"2018-01-11 00:00:00\", \"userremark\": null, \"ziti\": \"0\", \"orderftime\": " +
            "null, \"chuna\": null, \"jyn\": \"0\", \"usertruep\": \"0.0\", \"tidate\": null, " +
            "\"premark\": null, \"userorderid\": \"70413947481\", \"fp\": null, \"ucounty\": " +
            "\"1526\", \"storeid\": null, \"oprator\": \"600\", \"sendpay\": " +
            "\"00000000200000000000000002001000030030100000000000500000000001030000000000300000000000000000000101000000000000000000000000000000000000000000000000000000000000000000010000000000000000000100020000000010\", \"kt\": null, \"townId\": \"31420\", \"yun\": \"0.0\", \"ext0\": null, \"code\": null, \"payremk\": \"\", \"state2\": \"9\", \"ext1\": null, \"ext2\": null, \"ext3\": null, \"treatedsum\": null, \"treatedRemark\": null, \"resivedate\": null, \"schooluid\": \"\", \"jifen\": \"0\", \"splitType\": \"7\", \"fhy\": null, \"operren\": null, \"lastyu\": null, \"state\": \"5\", \"youhui\": \"0.0\", \"outdate\": null, \"pyt\": null, \"id\": \"70413947769\", \"printx\": \"1\", \"printtime\": null, \"cky2\": \"5\", \"di\": \"70\", \"newyu\": null, \"printy\": null, \"cky3\": null, \"totalServiceFee\": \"0.0\", \"zioper\": null, \"zititime\": null, \"memberID\": \"jd_47762beeec83b\", \"createDate\": \"2018-01-11 00:00:00\", \"treatedDate\": \"2018-01-11 00:00:00\", \"dabaotime\": null, \"totalPrice\": \"295.0\", \"ztdate\": null, \"shdate\": null, \"payment\": \"4\", \"executor\": null, \"operq\": null, \"orderpuyn\": null, \"paydate\": null, \"ucity\": \"1522\", \"updateDate\": \"2018-01-11 00:00:00\", \"orderfren\": null, \"ztyn\": null, \"trueyun\": null, \"yn\": \"1\", \"schoolId\": \"0\", \"Cky\": null}, \"cur\": {\"id\": \"70413947769\", \"yn\": \"0\", \"updateDate\": \"2018-01-12 00:00:03\"}, \"cus\": {\"p_ts\": \"1515686405021\", \"ft\": \"1515686403948\", \"ip\": \"172.28.205.127\"}} - 14";
    String key = "sendpay,id,memberID,customername,address,Uprovince,ucity,ucounty,schoolid," +
            "schooluid,code,phone,usermob,email,payment,di,remark,createDate,state,state2," +
            "treatedDate,treatedsum,treatedRemark,yn,totalPrice,userremark,yun,usertruep," +
            "orderfid,orderfren,orderftime,orderftel,orderpuyn,paydate,outdate,paysuredate," +
            "resivedate,youhui,lastyu,newyu,trueyun,oprator,jifen,jyn,pyt,payremk,printtime," +
            "printx,premark,tidate,shdate,shdatesum,Cky,fhy,chuna,ztdate,ztyn,fp,ziti,zioper," +
            "zititime,dabaotime,autoremark,operren,operq,kt,hzbj,cky2,cky3,printy,ordtype," +
            "bankname,userorderid,storeid,parentid,splitType,townId,updateDate";


    for (String k : key.split(",", -1)) {
      if (!jdq.contains(k)) {
        System.out.println(k);
      }
    }


  }
}