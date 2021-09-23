package com.tqk.rocketmqdemo.scheduled;

import com.tqk.rocketmqdemo.myenum.TqkEnum;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

public class ScheduledMessageConsumer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer=new DefaultMQPushConsumer("ScheduledMessageConsumer");
        consumer.setNamesrvAddr(TqkEnum.IPPORT.getMsg());
        //todo 消息消费模式（默认集群消费）
        consumer.setMessageModel(MessageModel.CLUSTERING);
        //todo 指定消费开始偏移量（上次消费偏移量、最大偏移量、最小偏移量、启动时间戳）开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        /**
         * 订阅指定topic下tags分别等于TagA或TagC或TagD, 这里没有订阅TagB的消息,所以不会消费标签为TagB的消息，*代表不过滤 接受一切
         */
        consumer.subscribe("Scheduled001", "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                try {
                    for (MessageExt message : messages) {
                        // Print approximate delay time period
                        System.out.println("Receive message[msgId=" + message.getMsgId() + "] "
                                + (message.getStoreTimestamp() - message.getBornTimestamp()) + "ms later");
                    }
                }catch (Exception e) {
                    e.printStackTrace();
                    //没有成功  -- 到重试队列中来
                    System.out.println("没有成功  -- 到重试队列中来");
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;

            }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;

            }
        });
        consumer.start();
    }
}
