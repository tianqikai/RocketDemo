package com.tqk.rocketmqdemo.quickstart;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 异步消息
 */
public class AsyncProducer {
    static  final  int messageCount = 10;
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer=new DefaultMQProducer("Async");
        producer.setNamesrvAddr("110.42.146.236:9876");
        producer.start();
        final CountDownLatch countDownLatch = new CountDownLatch(messageCount);
        //发送异步失败时的重试次数(这里不重试)
        producer.setRetryTimesWhenSendAsyncFailed(0);
        for (int i=0; i<messageCount;i++){
            final  int index=0;
            final Message message=new Message("TopicTest","TagC","OrderID"+index,("异步消息"+index).getBytes(RemotingHelper.DEFAULT_CHARSET));
            //生产者异步发送
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    countDownLatch.countDown();
                    System.out.printf("%-10d OK %s %n", index, new String(message.getBody()));
                }

                @Override
                public void onException(Throwable throwable) {
                    countDownLatch.countDown();
                    System.out.printf("%-10d Exception %s %n", index, throwable);
                    throwable.printStackTrace();
                }
            });

        }
        countDownLatch.await(2, TimeUnit.SECONDS);
        producer.shutdown();
    }
}
