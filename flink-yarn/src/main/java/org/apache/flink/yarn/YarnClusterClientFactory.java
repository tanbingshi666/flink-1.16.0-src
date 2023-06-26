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

package org.apache.flink.yarn;

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.deployment.AbstractContainerizedClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;
import org.apache.flink.yarn.configuration.YarnLogConfigUtil;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import javax.annotation.Nullable;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link ClusterClientFactory} for a YARN cluster. */
@Internal
public class YarnClusterClientFactory
        extends AbstractContainerizedClusterClientFactory<ApplicationId> {

    @Override
    public boolean isCompatibleWith(Configuration configuration) {
        checkNotNull(configuration);
        final String deploymentTarget = configuration.getString(DeploymentOptions.TARGET);
        return YarnDeploymentTarget.isValidYarnTarget(deploymentTarget);
    }

    @Override
    public YarnClusterDescriptor createClusterDescriptor(Configuration configuration) {
        checkNotNull(configuration);

        // 获取 $FLINK_HONE/conf 目录路径 比如 /opt/app/flink-1.16.0/conf
        final String configurationDirectory = configuration.get(DeploymentOptionsInternal.CONF_DIR);
        // 设置 Yarn Log 日志文件到 Configuration
        // 从 $FLINK_HOME/conf 目录下找到 logback.xml 文件 并设置在 Configuration
        // 比如 key = $internal.yarn.log-config-file value = /opt/app/flink-1.16.0/conf/logback.xml
        YarnLogConfigUtil.setLogConfigFileInConfig(configuration, configurationDirectory);

        // 获取 Yarn 集群描述器 YarnClusterDescriptor
        return getClusterDescriptor(configuration);
    }

    @Nullable
    @Override
    public ApplicationId getClusterId(Configuration configuration) {
        checkNotNull(configuration);
        final String clusterId = configuration.getString(YarnConfigOptions.APPLICATION_ID);
        return clusterId != null ? ConverterUtils.toApplicationId(clusterId) : null;
    }

    @Override
    public Optional<String> getApplicationTargetName() {
        return Optional.of(YarnDeploymentTarget.APPLICATION.getName());
    }

    private YarnClusterDescriptor getClusterDescriptor(Configuration configuration) {
        // 创建 Yarn 客户端 (YarnClientImpl <- YarnClient)
        final YarnClient yarnClient = YarnClient.createYarnClient();
        // 1 从 Flink 程序任务过滤去相关 Yarn 配置(前缀 flink.yarn.)
        // 2 从系统加载 Hadoop Yarn 相关配置文件信息 (export HADOOP_CLASSPATH=`hadoop classpath`)
        final YarnConfiguration yarnConfiguration =
                Utils.getYarnAndHadoopConfiguration(configuration);

        // Yarn 核心 Service 服务的标配调用方法
        // 调用 YarnClientImpl.serviceInit()
        // 初始化一些属性值
        yarnClient.init(yarnConfiguration);
        // 调用 YarnClientImpl.serviceStart()
        // 获取 Yarn ResourceManager 的 RPC ClientRMService 服务 的一个客户端代理对象
        // 通讯协议接口为 ApplicationClientProtocol
        yarnClient.start();

        // 创建 Yarn 集群描述器 YarnClusterDescriptor
        return new YarnClusterDescriptor(
                configuration,
                yarnConfiguration,
                yarnClient,
                YarnClientYarnClusterInformationRetriever.create(yarnClient),
                false);
    }
}
