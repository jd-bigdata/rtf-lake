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

import com.jd.dw.rtf.writer.Constants;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author anjinlong
 * @create 2018-01-21 14:27
 * @description description
 **/
public class TestMultiThread {

  private static ThreadPoolExecutor executor = new ThreadPoolExecutor(Constants.CORE_POOL_SIZE,
          Constants.MAX_POOL_SIZE, 200L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue
          (2000), new ThreadPoolExecutor.CallerRunsPolicy());

  public static void main(String[] args) {


    for (int i = 1; i < 5; i++) {
      Task task = new Task(i);
      try {
        executor.execute(task);
      }catch (Exception e){
        System.out.println("捕获到异常");
      }
    }

    //executor.shutdown();


  }

  static class Task implements Runnable {
    int i = 0;

    public Task(int i) {
      this.i = i;
    }

    public void run() {
      System.out.println(i);
      if(i == 3){
//        throw new RuntimeException("失败 线程退出");
        System.exit(-1);
      }
    }
  }


}