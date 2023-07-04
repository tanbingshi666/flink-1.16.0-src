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

package org.apache.flink.runtime.rest;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobmaster.MiniDispatcherRestEndpoint;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;

import java.util.concurrent.ScheduledExecutorService;

/** {@link RestEndpointFactory} which creates a {@link MiniDispatcherRestEndpoint}. */
public enum JobRestEndpointFactory implements RestEndpointFactory<RestfulGateway> {
    INSTANCE;

    @Override
    public WebMonitorEndpoint<RestfulGateway> createRestEndpoint(
            Configuration configuration,
            LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever,
            LeaderGatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
            TransientBlobService transientBlobService,
            ScheduledExecutorService executor,
            MetricFetcher metricFetcher,
            LeaderElectionService leaderElectionService,
            FatalErrorHandler fatalErrorHandler)
            throws Exception {
        // 根据配置参数 封装 RestEndpoint 相关参数 RestHandlerConfiguration
        /**
         * 1 刷新间隔 默认 3s
         * 2 最大缓存 checkpoint 数据 默认 10
         * 3 web 超时时间 默认 10min
         * 4 是否开启基于 web-ui 提交任务 默认 true
         * 5 是否开启基于 web-ui 取消任务 默认 true
         */
        final RestHandlerConfiguration restHandlerConfiguration =
                RestHandlerConfiguration.fromConfiguration(configuration);

        // 创建 MiniDispatcherRestEndpoint
        return new MiniDispatcherRestEndpoint(
                dispatcherGatewayRetriever,
                configuration,
                restHandlerConfiguration,
                resourceManagerGatewayRetriever,
                transientBlobService,
                executor,
                metricFetcher,
                leaderElectionService,
                RestEndpointFactory.createExecutionGraphCache(restHandlerConfiguration),
                fatalErrorHandler);
    }
}
