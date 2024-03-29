/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.base.source.reader.fetcher;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A Fetcher Manager with a single fetching thread (I/O thread) that handles all splits
 * concurrently.
 *
 * <p>This pattern is, for example, useful for connectors like File Readers, Apache Kafka Readers,
 * etc. In the example of Kafka, there is a single thread that reads all splits (topic partitions)
 * via the same client. In the example of the file source, there is a single thread that reads the
 * files after another.
 */
@Internal
public class SingleThreadFetcherManager<E, SplitT extends SourceSplit>
        extends SplitFetcherManager<E, SplitT> {

    /**
     * Creates a new SplitFetcherManager with a single I/O threads.
     *
     * @param elementsQueue The queue that is used to hand over data from the I/O thread (the
     *     fetchers) to the reader (which emits the records and book-keeps the state. This must be
     *     the same queue instance that is also passed to the {@link SourceReaderBase}.
     * @param splitReaderSupplier The factory for the split reader that connects to the source
     *     system.
     */
    public SingleThreadFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            Supplier<SplitReader<E, SplitT>> splitReaderSupplier) {
        // 往下追
        super(elementsQueue, splitReaderSupplier);
    }

    /**
     * Creates a new SplitFetcherManager with a single I/O threads.
     *
     * @param elementsQueue The queue that is used to hand over data from the I/O thread (the
     *     fetchers) to the reader (which emits the records and book-keeps the state. This must be
     *     the same queue instance that is also passed to the {@link SourceReaderBase}.
     * @param splitReaderSupplier The factory for the split reader that connects to the source
     *     system.
     * @param splitFinishedHook Hook for handling finished splits in split fetchers
     */
    @VisibleForTesting
    public SingleThreadFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            Supplier<SplitReader<E, SplitT>> splitReaderSupplier,
            Consumer<Collection<String>> splitFinishedHook) {
        super(elementsQueue, splitReaderSupplier, splitFinishedHook);
    }

    @Override
    public void addSplits(List<SplitT> splitsToAdd) {
        // 1 获取正在运行的 SplitFetcher 线程
        SplitFetcher<E, SplitT> fetcher = getRunningFetcher();
        // 2 如果没有正在运行的 SplitFetcher 线程 则创建
        if (fetcher == null) {
            // 2.1 创建 SplitFetcher 线程
            // SplitFetcher 线程添加对应的 SplitReader 读取数据器
            fetcher = createSplitFetcher();
            // Add the splits to the fetchers.
            // 2.2 将申请到的 splits 添加到 SplitFetcher 线程
            fetcher.addSplits(splitsToAdd);
            // 2.3 启动 SplitFetcher 线程
            startFetcher(fetcher);
        } else {
            // 3 如果已经存在有了 SplitFetcher 则获取
            // 一般情况下 FetcherManager 只维护了一个 SplitFetcher 线程
            fetcher.addSplits(splitsToAdd);
        }
    }

    protected SplitFetcher<E, SplitT> getRunningFetcher() {
        return fetchers.isEmpty() ? null : fetchers.values().iterator().next();
    }
}
