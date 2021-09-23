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

import com.tqk.rocketmqdemo.myenum.TqkEnum;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.*;

/**
 * 事务消息-生产者 A
 */
public class TransactionProducer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        //创建事务监听器
        TransactionListener transactionListener = new TransactionListenerImpl();
        //创建消息生产者
        TransactionMQProducer producer = new TransactionMQProducer("TransactionProducer");
        // 设置NameServer的地址
        producer.setNamesrvAddr(TqkEnum.IPPORT.getMsg());
        //创建线程池
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        });
        //设置生产者回查线程池
        producer.setExecutorService(executorService);
        //生产者设置监听器
        producer.setTransactionListener(transactionListener);
        //启动消息生产者

        producer.start();
        //todo  1、开启事务
        //todo @Transaction
        System.out.println("开启事务@Transaction");
//        try {
//            Message msg =
//                new Message("TransactionTopic", null, ("A向B系统转100块钱 ").getBytes(RemotingHelper.DEFAULT_CHARSET));
//            //todo 半事务的发送
//            SendResult sendResult = producer.sendMessageInTransaction(msg, null);
//            sendResult.getMsgId();//MQ生成的
//        } catch (MQClientException | UnsupportedEncodingException e) {
//            //todo 回滚rollback
//            e.printStackTrace();
//        }
        String[] tags = new String[] {"TagA", "TagB", "TagC"};
        for (int i = 0; i < 3; i++) {
            try {
                Message msg =
                        new Message("TransactionTopic", tags[i % tags.length], "KEY" + i,
                                ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                //1,2步  半事务的发送，确认。
                SendResult sendResult = producer.sendMessageInTransaction(msg, null);
                System.out.printf("%s%n", sendResult);
                Thread.sleep(1000);
            } catch (MQClientException | UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

        //等待，因为要等输入密码，确认等操作，因为要事务回查，
        for (int i = 0; i < 1000; i++) {
            Thread.sleep(1000);
        }
        producer.shutdown();
    }
}
