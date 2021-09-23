package com.tqk.rocketmqdemo.scheduled;

import com.tqk.rocketmqdemo.myenum.TqkEnum;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.nio.charset.StandardCharsets;

/**
 * @author Administrator
 * 延时消息
 */
public class ScheduledMessageProducer {
    public static void main(String[] args) throws InterruptedException {
        DefaultMQProducer producer=new DefaultMQProducer("ScheduledMessageTqk");
        producer.setNamesrvAddr(TqkEnum.IPPORT.getMsg());
        try {
            producer.start();
            int totalSendMsg=10;
            for (int i = 0; i < totalSendMsg; i++) {
                Message message=new Message("Scheduled001","tag01",("helloScheduledMessageTqk "+i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                // 设置延时等级3,这个消息将在10s之后投递给消费者(详看delayTimeLevel)
                // delayTimeLevel：(1~18个等级)"1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"
                message.setDelayTimeLevel(4);
                producer.send(message);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        Thread.sleep(1000);
        producer.shutdown();
    }
}
