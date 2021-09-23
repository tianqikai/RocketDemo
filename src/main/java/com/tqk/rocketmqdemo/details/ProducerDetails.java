package com.tqk.rocketmqdemo.details;

import com.tqk.rocketmqdemo.myenum.TqkEnum;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

public class ProducerDetails {
    public static void main(String[] args) throws Exception {
        //todo producerGroup：生产者所属组(针对 事务消息 高可用)
        DefaultMQProducer producer=new DefaultMQProducer("produce_details");
        //todo 默认主题在每一个Broker队列数量(对于新创建主题有效)
        producer.setDefaultTopicQueueNums(8);
        //todo 发送消息默认超时时间，默认3s (3000ms)
        producer.setSendMsgTimeout(3000);
        //todo 消息体超过该值则启用压缩，默认4k
        producer.setCompressMsgBodyOverHowmuch(4*1024);
        //todo 异步方式发送消息重试次数，默认为2，总共执行3次
        producer.setRetryTimesWhenSendAsyncFailed(2);
        //todo 同步方式发送消息重试次数，默认为2，总共执行3次
        producer.setRetryTimesWhenSendFailed(2);
        //todo 消息重试时选择另外一个Broker时（消息没有存储成功是否发送到另外一个broker），默认为false
        producer.setRetryAnotherBrokerWhenNotStoreOK(false);
        //todo 允许发送的最大消息长度，默认为4M
        producer.setMaxMessageSize(1024 * 1024 * 4);
        //todo 设置NameServer的地址
        producer.setNamesrvAddr(TqkEnum.IPPORT.getMsg());
        producer.start();
        //todo 0 查找该主题下所有消息队列
        List<MessageQueue> messageQueueNow = producer.fetchPublishMessageQueues("DetailsNow");

        List<MessageQueue> messageQueue = producer.fetchPublishMessageQueues("DetailsOneway");
        for (int i = 0; i < messageQueue.size(); i++) {
            System.out.println(messageQueue.get(i).getQueueId());
        }
        for (int i = 0; i < 10; i++){
            final int index = i;
            // 创建消息，并指定Topic，Tag和消息体
            Message msg = new Message("DetailsOneway", "TagA", "OrderID888",
                    "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));

            //todo 单向发送
            //todo 1.1发送单向消息
            producer.sendOneway(msg);
            //todo 1.2指定队列单向发送消息(使用select方法)
            producer.sendOneway(msg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    return mqs.get(0);
                }
            },null);
            //todo 1.3指定队列单向发送消息(根据之前查找出来的主题)
            producer.sendOneway(msg,messageQueue.get(1));

            //todo 同步发送
            // 创建消息，并指定Topic，Tag和消息体
            Message nowMsg = new Message("DetailsNow", "TagA", "OrderID888",
                    "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
            //todo 2.1同步发送消息
            SendResult sendResult0 = producer.send(nowMsg);
            //todo 2.1同步超时发送消息(属性设置：sendMsgTimeout 发送消息默认超时时间，默认3s (3000ms) )
            SendResult sendResult1 = producer.send(nowMsg,1000*3);
            //todo 2.2指定队列同步发送消息(使用select方法)
            SendResult sendResult2 = producer.send(nowMsg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    return messageQueueNow.get(0);
                }
            },null);
            producer.send(nowMsg,messageQueueNow.get(3));


            //todo 异步发送
            // 创建消息，并指定Topic，Tag和消息体
            Message asynMsg = new Message("DetailsAsyn", "TagA", "OrderID888",
                    "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
            List<MessageQueue> messageQueueAsyn = producer.fetchPublishMessageQueues("DetailsAsyn");

            //todo 3.1异步发送消息
            producer.send(asynMsg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("%-10d OK %s %n", index, sendResult.getMsgId());
                }
                @Override
                public void onException(Throwable e) {
                    System.out.printf("%-10d Exception %s %n", index, e);e.printStackTrace();
                }
            });
            //todo 3.1异步超时发送消息
            producer.send(asynMsg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("%-10d OK %s %n", index, sendResult.getMsgId());
                }
                @Override
                public void onException(Throwable e) {
                    System.out.printf("%-10d Exception %s %n", index, e);e.printStackTrace();
                }
            },1000*3);
            //todo 3.2选择指定队列异步发送消息(根据之前查找出来的主题队列信息)
            producer.send(asynMsg,messageQueueAsyn.get(0),
                    new SendCallback() {
                        @Override
                        public void onSuccess(SendResult sendResult) {
                            System.out.printf("%-10d OK %s %n", index, sendResult.getMsgId());
                        }
                        @Override
                        public void onException(Throwable e) {
                            System.out.printf("%-10d Exception %s %n", index, e);e.printStackTrace();
                        }
                    });
            //todo 3.3选择指定队列异步发送消息(使用select方法)
            producer.send(asynMsg,new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                            return messageQueueAsyn.get(5);
                        }
                    },
                    new SendCallback() {
                        @Override
                        public void onSuccess(SendResult sendResult) {
                            System.out.printf("%-10d OK %s %n", index, sendResult.getMsgId());
                        }
                        @Override
                        public void onException(Throwable e) {
                            System.out.printf("%-10d Exception %s %n", index, e);e.printStackTrace();
                        }
                    });
        }
        Thread.sleep(10000);
        //todo  如果不再发送消息，关闭Producer实例。
        producer.shutdown();

    }
}
