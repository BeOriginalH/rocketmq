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
package org.apache.rocketmq.client.impl.producer;

import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;

public class TopicPublishInfo{

    private boolean orderTopic = false;

    private boolean haveTopicRouterInfo = false;

    private List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>();

    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();

    private TopicRouteData topicRouteData;

    public boolean isOrderTopic() {

        return orderTopic;
    }

    public void setOrderTopic(boolean orderTopic) {

        this.orderTopic = orderTopic;
    }

    public boolean ok() {

        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }

    public List<MessageQueue> getMessageQueueList() {

        return messageQueueList;
    }

    public void setMessageQueueList(List<MessageQueue> messageQueueList) {

        this.messageQueueList = messageQueueList;
    }

    public ThreadLocalIndex getSendWhichQueue() {

        return sendWhichQueue;
    }

    public void setSendWhichQueue(ThreadLocalIndex sendWhichQueue) {

        this.sendWhichQueue = sendWhichQueue;
    }

    public boolean isHaveTopicRouterInfo() {

        return haveTopicRouterInfo;
    }

    public void setHaveTopicRouterInfo(boolean haveTopicRouterInfo) {

        this.haveTopicRouterInfo = haveTopicRouterInfo;
    }

    /**
     * 选择一个消息队列，当第一次调用的时候，直接根据队列取模获取队列，后续则需要检查选取出来的broker是否是上一次的，
     * 如果是则跳过选取下一个，因lastBrokerName是发送消息失败的broker，也就是故障的broker
     *
     * @param lastBrokerName
     * @return
     */
    public MessageQueue selectOneMessageQueue(final String lastBrokerName) {

        if (lastBrokerName == null) {
            return selectOneMessageQueue();
        } else {
            int index = this.sendWhichQueue.getAndIncrement();
            for (int i = 0; i < this.messageQueueList.size(); i++) {
                int pos = Math.abs(index++) % this.messageQueueList.size();
                if (pos < 0)
                    pos = 0;
                MessageQueue mq = this.messageQueueList.get(pos);
                if (!mq.getBrokerName().equals(lastBrokerName)) {
                    return mq;
                }
            }
            return selectOneMessageQueue();
        }
    }

    /**
     * 选择一个消息队列，每次取不同的一个，通过一个累加计数器实现
     *
     * @return
     */
    public MessageQueue selectOneMessageQueue() {

        int index = this.sendWhichQueue.getAndIncrement();
        int pos = Math.abs(index) % this.messageQueueList.size();
        if (pos < 0)
            pos = 0;
        return this.messageQueueList.get(pos);
    }

    public int getQueueIdByBroker(final String brokerName) {

        for (int i = 0; i < topicRouteData.getQueueDatas().size(); i++) {
            final QueueData queueData = this.topicRouteData.getQueueDatas().get(i);
            if (queueData.getBrokerName().equals(brokerName)) {
                return queueData.getWriteQueueNums();
            }
        }

        return -1;
    }

    @Override
    public String toString() {

        return "TopicPublishInfo [orderTopic=" + orderTopic + ", messageQueueList=" + messageQueueList
            + ", sendWhichQueue=" + sendWhichQueue + ", haveTopicRouterInfo=" + haveTopicRouterInfo + "]";
    }

    public TopicRouteData getTopicRouteData() {

        return topicRouteData;
    }

    public void setTopicRouteData(final TopicRouteData topicRouteData) {

        this.topicRouteData = topicRouteData;
    }
}
