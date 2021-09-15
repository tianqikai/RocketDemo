package com.tqk.rocketmqdemo.quickstart;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * * 同步发送
 */
public class SyncProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer=new DefaultMQProducer("sync");
        producer.setNamesrvAddr("110.42.146.236:9876");
        producer.start();
        for (int i = 0; i < 10; i++) {
            Message msg = new Message("TopicTest" ,
                    "TagB" ,
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET)
            );
            SendResult sendResult = producer.send(msg);
            System.out.println("SendStatus:"+sendResult.getSendStatus()+"  (MsgId):"
                    +sendResult.getMsgId()+"  (queueId):"
                    +sendResult.getMessageQueue().getQueueId()
                    +"  (value):"+ new String(msg.getBody()));
        }
        producer.shutdown();
    }
}
