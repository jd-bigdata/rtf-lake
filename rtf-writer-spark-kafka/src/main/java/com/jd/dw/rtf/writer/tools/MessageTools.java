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

package com.jd.dw.rtf.writer.tools;

/**
 * @author anjinlong
 * @create 2017-11-24 10:19
 * @description description
 **/
public class MessageTools {

  public static void main(String[] args) {
    generate();
  }


  public static void generate() {
    String t = "m_id             \n" +
            "waybill_code     \n" +
            "actual_collection\n" +
            "collection_type  \n" +
            "waybill_state    \n" +
            "sign_state       \n" +
            "sign_has_return  \n" +
            "waybill_type     \n" +
            "create_user      \n" +
            "create_user_id   \n" +
            "create_time      \n" +
            "create_site      \n" +
            "later_user_id    \n" +
            "later_user       \n" +
            "cancel_reason    \n" +
            "print            \n" +
            "yn               \n" +
            "update_time      \n" +
            "waybill_flag     \n" +
            "site_id          \n" +
            "site_name        \n" +
            "site_type        \n" +
            "storeid          \n" +
            "cky2             \n" +
            "source_cky2      \n" +
            "waybill_org_id   \n" +
            "first_time       \n" +
            "operate_time     \n" +
            "start_date       \n" +
            "change_code      \n" +
            "sort_site_id     \n" +
            "flag_operate_time\n" +
            "reverse_site_id  \n";

    String[] ts = t.split("\n");
    for (String tmp : ts) {
      System.out.println(tmp.trim());
    }


  }

}