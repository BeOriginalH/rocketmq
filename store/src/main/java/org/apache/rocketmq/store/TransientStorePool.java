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
package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * 暂存池 用来临时存放数据，数据先写入该内存映射中，然后通过commit线程定时将数据提交到与目标物理文件对应的内存映射文件中
 *
 * 主要是提供一种内存锁定机制，将当前的堆外内存一直锁定到内存中，避免被进程将内存交换到磁盘
 */
public class TransientStorePool {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 预分配的ByteBuffer的数量 默认5个
     */
    private final int poolSize;

    /**
     * 每个ByteBuffer的大小
     */
    private final int fileSize;

    /**
     * 可用的ByteBuffer
     */
    private final Deque<ByteBuffer> availableBuffers;

    /**
     * 消息存储配置文件
     */
    private final MessageStoreConfig storeConfig;

    public TransientStorePool(final MessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        this.poolSize = storeConfig.getTransientStorePoolSize();
        this.fileSize = storeConfig.getMappedFileSizeCommitLog();
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }

    /**
     * 初始化预分配池
     * It's a heavy init method.
     */
    public void init() {
        for (int i = 0; i < poolSize; i++) {
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);

            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            //锁住内存，避免操作系统虚拟内存的换入换出
            LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));

            availableBuffers.offer(byteBuffer);
        }
    }

    /**
     * 销毁内存池
     */
    public void destroy() {
        for (ByteBuffer byteBuffer : availableBuffers) {
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
    }

    /**
     * 使用完毕后归还ByteBuffer
     * @param byteBuffer
     */
    public void returnBuffer(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        byteBuffer.limit(fileSize);
        this.availableBuffers.offerFirst(byteBuffer);
    }

    /**
     * 获取ByteBuffer
     * @return
     */
    public ByteBuffer borrowBuffer() {
        ByteBuffer buffer = availableBuffers.pollFirst();
        if (availableBuffers.size() < poolSize * 0.4) {
            log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }

    /**
     * 可用ByteBuffer的数量
     * @return
     */
    public int availableBufferNums() {
        if (storeConfig.isTransientStorePoolEnable()) {//如果启用了暂存池，则返回队列中的元素个数
            return availableBuffers.size();
        }
        return Integer.MAX_VALUE;
    }
}
