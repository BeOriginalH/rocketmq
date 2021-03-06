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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * 文件映射文件
 */
public class MappedFile extends ReferenceResource {

    /**
     * 操作系统每页大小 4K
     */
    public static final int OS_PAGE_SIZE = 1024 * 4;

    /**
     * 内置的日志文件
     */
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 当前JVM中MappedFile虚拟内存的总共大小累加器
     */
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    /**
     * 当前JVM中MappedFile的数量累加器
     */
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

    /**
     * 当前文件的写指针
     */
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);

    /**
     * 当前文件的提交指针
     */
    protected final AtomicInteger committedPosition = new AtomicInteger(0);

    /**
     * 刷盘指针
     */
    private final AtomicInteger flushedPosition = new AtomicInteger(0);

    /**
     * 文件大小
     */
    protected int fileSize;

    /**
     * 该MappedFile文件对应的channel
     */
    protected FileChannel fileChannel;

    /**
     * 堆内ByteBuffer
     *
     * 只有在开启暂存池(transientStorePoolEnable=true)的时候才会初始化该buffer
     *
     * 消息保存的时候，先将消息保存到该writerBuffer中，在通过commit线程将消息提交到fileChannel中
     *
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     */
    protected ByteBuffer writeBuffer = null;

    /**
     * 内部使用ByteBuffer实现的字节池，如果启用了transientStorePool，则不能为空
     */
    protected TransientStorePool transientStorePool = null;

    /**
     * 文件名称
     */
    private String fileName;

    /**
     * 文件起始偏移量
     */
    private long fileFromOffset;

    /**
     * 该MappedFile对应的真实文件
     */
    private File file;

    /**
     * 物理文件对应的内存映射buffer
     */
    private MappedByteBuffer mappedByteBuffer;

    /**
     * 存储时间
     */
    private volatile long storeTimestamp = 0;

    /**
     * 是否是MappedFileQueue中的第一个文件
     */
    private boolean firstCreateInQueue = false;

    public MappedFile() {

    }

    /**
     * 构造器
     *
     * @param fileName 下一个文件的名称
     * @param fileSize 文件大小
     * @throws IOException
     */
    public MappedFile(final String fileName, final int fileSize) throws IOException {

        init(fileName, fileSize);
    }

    public MappedFile(final String fileName, final int fileSize, final TransientStorePool transientStorePool)
            throws IOException {

        init(fileName, fileSize, transientStorePool);
    }

    public static void ensureDirOK(final String dirName) {

        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public static void clean(final ByteBuffer buffer) {

        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0) {
            return;
        }
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {

        return AccessController.doPrivileged(new PrivilegedAction<Object>() {

            public Object run() {

                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args) throws NoSuchMethodException {

        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {

        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null) {
            return buffer;
        } else {
            return viewed(viewedBuffer);
        }
    }

    public static int getTotalMappedFiles() {

        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {

        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    public void init(final String fileName, final int fileSize, final TransientStorePool transientStorePool)
            throws IOException {

        init(fileName, fileSize);
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    /**
     * 初始化
     *
     * @param fileName
     * @param fileSize
     * @throws IOException
     */
    private void init(final String fileName, final int fileSize) throws IOException {

        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        ensureDirOK(this.file.getParent());

        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            //更新统计计数器
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {

        return this.file.lastModified();
    }

    public int getFileSize() {

        return fileSize;
    }

    public FileChannel getFileChannel() {

        return fileChannel;
    }

    /**
     * 添加消息
     *
     * @param msg
     * @param cb
     * @return
     */
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {

        return appendMessagesInner(msg, cb);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {

        return appendMessagesInner(messageExtBatch, cb);
    }

    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {

        assert messageExt != null;
        assert cb != null;
        //已写入点
        int currentPos = this.wrotePosition.get();

        if (currentPos < this.fileSize) {//如果文件未写满
            //如果writeBuffer不为null,则使用writeBuffer，否则使用mappedByteBuffer
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result;
            if (messageExt instanceof MessageExtBrokerInner) {//普通消息

                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                        (MessageExtBrokerInner) messageExt);

            } else if (messageExt instanceof MessageExtBatch) {//批量消息

                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                        (MessageExtBatch) messageExt);

            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            //更新已写入点
            this.wrotePosition.addAndGet(result.getWroteBytes());
            //记录上次写入的时间点
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        //如果文件已经写满，抛出异常
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {

        return this.fileFromOffset;
    }

    public boolean appendMessage(final byte[] data) {

        int currentPos = this.wrotePosition.get();

        if ((currentPos + data.length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {

        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * 将内存中的数据永久的写入磁盘
     *
     * @return The current flushed position
     */
    public int flush(final int flushLeastPages) {

        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();

                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        //false代表只需要将文件文件内容更新写入存储，true代表还需要写入元数据
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    /**
     * 提交 主要是针对开启暂存池的，将writeBuffer中的数据提交到内存映射buffer中
     *
     * @param commitLeastPages
     * @return
     */
    public int commit(final int commitLeastPages) {

        if (writeBuffer == null) {//如果不是开启了暂存池，不需要提交
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }
        if (this.isAbleToCommit(commitLeastPages)) {//判断是否可以提交
            if (this.hold()) {
                //提交
                commit0(commitLeastPages);
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        //如果本文件已经写满，将writerBuffer归还暂存池
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return this.committedPosition.get();
    }

    /**
     * @param commitLeastPages
     */
    protected void commit0(final int commitLeastPages) {

        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - this.committedPosition.get() > 0) {//提交点和写入点不同步
            try {
                //将writeBuffer中上一次提交后的信息到写入点的信息提交到fileChannel中，并且更新提交点
                ByteBuffer byteBuffer = writeBuffer.slice();
                byteBuffer.position(lastCommittedPosition);
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    /**
     * 是否可以刷盘
     *
     * @param flushLeastPages
     * @return
     */
    private boolean isAbleToFlush(final int flushLeastPages) {

        int flush = this.flushedPosition.get();
        int write = getReadPosition();

        if (this.isFull()) {//如果MappedFile已经写满，可以刷盘
            return true;
        }

        if (flushLeastPages > 0) {//
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    /**
     * 判断是否可以提交
     *
     * @param commitLeastPages
     * @return
     */
    protected boolean isAbleToCommit(final int commitLeastPages) {

        int flush = this.committedPosition.get();
        int write = this.wrotePosition.get();

        if (this.isFull()) {
            return true;
        }

        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > flush;
    }

    public int getFlushedPosition() {

        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {

        this.flushedPosition.set(pos);
    }

    public boolean isFull() {

        return this.fileSize == this.wrotePosition.get();
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {

        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: " + this.fileFromOffset);
            }
        } else {
            log.warn(
                    "selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size + ", fileFromOffset: "
                            + this.fileFromOffset);
        }

        return null;
    }

    /**
     * 查找指定位置到当前文件最大可以读取的位置之间的内容
     * @param pos
     * @return
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos) {

        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                //todo  如果是使用writeBuffer来保存的，数据是保存到filechannel中的，如何获取数据
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    /**
     * 清理
     *
     * @param currentRef
     * @return
     */
    @Override
    public boolean cleanup(final long currentRef) {

        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName + " have cleanup, do not do it again.");
            return true;
        }

        clean(this.mappedByteBuffer);
        //统计数据
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    public boolean destroy(final long intervalForcibly) {

        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info(
                        "delete file[REF:" + this.getRefCount() + "] " + this.fileName + (result ? " OK, " : " Failed, ")
                                + "W:" + this.getWrotePosition() + " M:" + this.getFlushedPosition() + ", " + UtilAll
                                .computeElapsedTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName + " Failed. cleanupOver: "
                    + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {

        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {

        this.wrotePosition.set(pos);
    }

    /**
     * 获取读取的位置
     * @return The max position which have valid data
     */
    public int getReadPosition() {

        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {

        this.committedPosition.set(pos);
    }

    public void warmMappedFile(FlushDiskType type, int pages) {

        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}", this.getFileName(),
                    System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
                System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    public String getFileName() {

        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {

        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {

        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {

        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {

        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {

        this.firstCreateInQueue = firstCreateInQueue;
    }

    public void mlock() {

        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret,
                    System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret,
                    System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {

        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret,
                System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {

        return this.file;
    }

    @Override
    public String toString() {

        return this.fileName;
    }
}
