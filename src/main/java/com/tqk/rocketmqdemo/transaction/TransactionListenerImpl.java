/*
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
package com.tqk.rocketmqdemo.transaction;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 事务监听器
 *
 */
public class TransactionListenerImpl implements TransactionListener {
    // 事务状态记录
    private AtomicInteger transactionIndex = new AtomicInteger(0);
    private ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<>();
    // 执行本地事务 3
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        System.out.println("执行本地事务");
        int value = transactionIndex.getAndIncrement();
        //0,1,2
        int status = value % 3;
        localTrans.put(msg.getTransactionId(), status);
        //这里模拟的不进行步骤4  A系统不知道的--UNKNOW
//      return LocalTransactionState.UNKNOW;
        switch (status) {
            case 0:
                System.out.println("MQ返回消息【"+msg.getTransactionId()+"】事务状态【中间状态】");
                return LocalTransactionState.UNKNOW;
            case 1:
                System.out.println("MQ返回消息【"+msg.getTransactionId()+"】事务状态【提交状态】");
                return LocalTransactionState.COMMIT_MESSAGE;
            case 2:
                System.out.println("MQ返回消息【"+msg.getTransactionId()+"】事务状态【回滚状态】");
                return LocalTransactionState.ROLLBACK_MESSAGE;
            default:
                System.out.println("MQ返回消息【"+msg.getTransactionId()+"】事务状态【状态未知】");
                return LocalTransactionState.UNKNOW;
        }
    }

    /**
     * 检查本地事务状态  默认是60s，一分钟检查一次
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        //打印每次回查的时间
        //设置日期格式
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        // new Date()为获取当前系统时间
        System.out.println("checkLocalTransaction:"+df.format(new Date()));
        Integer status = localTrans.get(msg.getTransactionId());
        if (null != status) {
            switch (status) {
                case 0:
                    System.out.println("MQ检查消息【"+msg.getTransactionId()+"】事务状态【中间状态】");
                    return LocalTransactionState.UNKNOW;
                case 1:
                    System.out.println("MQ检查消息【"+msg.getTransactionId()+"】事务状态【提交状态】");
                    return LocalTransactionState.COMMIT_MESSAGE;
                case 2:
                    System.out.println("MQ检查消息【"+msg.getTransactionId()+"】事务状态【回滚状态】");
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                default:
                    System.out.println("MQ检查消息【"+msg.getTransactionId()+"】事务状态【状态未知】");
                    return LocalTransactionState.UNKNOW;
            }
        }
      //  System.out.println("MQ检查消息【"+msg.getTransactionId()+"】事务状态【提交状态】");
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}
