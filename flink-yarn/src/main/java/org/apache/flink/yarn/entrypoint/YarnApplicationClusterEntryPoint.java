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

package org.apache.flink.yarn.entrypoint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.deployment.application.ApplicationClusterEntryPoint;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.DefaultPackagedProgramRetriever;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramRetriever;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.entrypoint.DynamicParametersConfigurationParserFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** An {@link ApplicationClusterEntryPoint} for Yarn. */
@Internal
public final class YarnApplicationClusterEntryPoint extends ApplicationClusterEntryPoint {

    private YarnApplicationClusterEntryPoint(
            final Configuration configuration, final PackagedProgram program) {
        // 往下追
        super(configuration, program, YarnResourceManagerFactory.getInstance());
    }

    @Override
    protected String getRPCPortRange(Configuration configuration) {
        return configuration.getString(YarnConfigOptions.APPLICATION_MASTER_PORT);
    }

    /**
     * #!/bin/bash
     * <p>
     * set -o pipefail -e
     * export PRELAUNCH_OUT="/opt/app/hadoop-3.1.3/logs/userlogs/application_1687757067625_0009/container_1687757067625_0009_01_000001/prelaunch.out"
     * exec >"${PRELAUNCH_OUT}"
     * export PRELAUNCH_ERR="/opt/app/hadoop-3.1.3/logs/userlogs/application_1687757067625_0009/container_1687757067625_0009_01_000001/prelaunch.err"
     * exec 2>"${PRELAUNCH_ERR}"
     * echo "Setting up env variables"
     * export JAVA_HOME=${JAVA_HOME:-"/opt/app/jdk1.8.0_144"}
     * export HADOOP_COMMON_HOME=${HADOOP_COMMON_HOME:-"/opt/app/hadoop-3.1.3"}
     * export HADOOP_HDFS_HOME=${HADOOP_HDFS_HOME:-"/opt/app/hadoop-3.1.3"}
     * export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-"/opt/app/hadoop-3.1.3/etc/hadoop"}
     * export HADOOP_YARN_HOME=${HADOOP_YARN_HOME:-"/opt/app/hadoop-3.1.3"}
     * export HADOOP_MAPRED_HOME=${HADOOP_MAPRED_HOME:-"/opt/app/hadoop-3.1.3"}
     * export HADOOP_TOKEN_FILE_LOCATION="/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/container_1687757067625_0009_01_000001/container_tokens"
     * export CONTAINER_ID="container_1687757067625_0009_01_000001"
     * export NM_PORT="43411"
     * export NM_HOST="node2"
     * export NM_HTTP_PORT="8042"
     * export LOCAL_DIRS="/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009"
     * export LOCAL_USER_DIRS="/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/"
     * export LOG_DIRS="/opt/app/hadoop-3.1.3/logs/userlogs/application_1687757067625_0009/container_1687757067625_0009_01_000001"
     * export USER="hdfs"
     * export LOGNAME="hdfs"
     * export HOME="/home/"
     * export PWD="/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/container_1687757067625_0009_01_000001"
     * export JVM_PID="$$"
     * export MALLOC_ARENA_MAX="4"
     * export NM_AUX_SERVICE_mapreduce_shuffle="AAA0+gAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
     * export _CLIENT_HOME_DIR="hdfs://hadoop101:8020/user/hdfs"
     * export APP_SUBMIT_TIME_ENV="1688102392929"
     * export _APP_ID="application_1687757067625_0009"
     * export HADOOP_USER_NAME="hdfs"
     * export APPLICATION_WEB_PROXY_BASE="/proxy/application_1687757067625_0009"
     * export FLINK_OPT_DIR="./opt"
     * export FLINK_LIB_DIR="./lib"
     * export _CLIENT_SHIP_FILES="YarnLocalResourceDescriptor{key=log4j.properties, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/log4j.properties, size=2694, modificationTime=1688102383462, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/flink-shaded-zookeeper-3.5.9.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/lib/flink-shaded-zookeeper-3.5.9.jar, size=10737871, modificationTime=1688102383986, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/log4j-api-2.17.1.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/lib/log4j-api-2.17.1.jar, size=301872, modificationTime=1688102384030, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/flink-json-1.16.0.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/lib/flink-json-1.16.0.jar, size=180250, modificationTime=1688102384061, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/flink-table-planner-loader-1.16.0.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/lib/flink-table-planner-loader-1.16.0.jar, size=36237974, modificationTime=1688102385158, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/log4j-1.2-api-2.17.1.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/lib/log4j-1.2-api-2.17.1.jar, size=208006, modificationTime=1688102385204, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/log4j-core-2.17.1.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/lib/log4j-core-2.17.1.jar, size=1790452, modificationTime=1688102385270, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/log4j-slf4j-impl-2.17.1.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/lib/log4j-slf4j-impl-2.17.1.jar, size=24279, modificationTime=1688102385309, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/flink-csv-1.16.0.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/lib/flink-csv-1.16.0.jar, size=102470, modificationTime=1688102385342, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/flink-table-api-java-uber-1.16.0.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/lib/flink-table-api-java-uber-1.16.0.jar, size=15367434, modificationTime=1688102386348, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/flink-scala_2.12-1.16.0.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/lib/flink-scala_2.12-1.16.0.jar, size=21052633, modificationTime=1688102387600, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/flink-cep-1.16.0.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/lib/flink-cep-1.16.0.jar, size=198855, modificationTime=1688102387626, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/flink-connector-files-1.16.0.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/lib/flink-connector-files-1.16.0.jar, size=515825, modificationTime=1688102387655, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/flink-table-runtime-1.16.0.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/lib/flink-table-runtime-1.16.0.jar, size=3133682, modificationTime=1688102387687, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/paimon-flink-1.16-0.4-SNAPSHOT.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/lib/paimon-flink-1.16-0.4-SNAPSHOT.jar, size=26712095, modificationTime=1688102388134, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/flink-sql-connector-mysql-cdc-2.3.0.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/lib/flink-sql-connector-mysql-cdc-2.3.0.jar, size=22968127, modificationTime=1688102389311, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/paimon-flink-action-0.4.0-incubating.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/lib/paimon-flink-action-0.4.0-incubating.jar, size=9511, modificationTime=1688102389374, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=plugins/metrics-statsd/flink-metrics-statsd-1.16.0.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/plugins/metrics-statsd/flink-metrics-statsd-1.16.0.jar, size=11745, modificationTime=1688102389900, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=plugins/metrics-prometheus/flink-metrics-prometheus-1.16.0.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/plugins/metrics-prometheus/flink-metrics-prometheus-1.16.0.jar, size=101507, modificationTime=1688102390321, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=plugins/metrics-graphite/flink-metrics-graphite-1.16.0.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/plugins/metrics-graphite/flink-metrics-graphite-1.16.0.jar, size=175859, modificationTime=1688102390342, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=plugins/external-resource-gpu/flink-external-resource-gpu-1.16.0.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/plugins/external-resource-gpu/flink-external-resource-gpu-1.16.0.jar, size=15731, modificationTime=1688102390363, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=plugins/external-resource-gpu/gpu-discovery-common.sh, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/plugins/external-resource-gpu/gpu-discovery-common.sh, size=3189, modificationTime=1688102390382, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=plugins/external-resource-gpu/nvidia-gpu-discovery.sh, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/plugins/external-resource-gpu/nvidia-gpu-discovery.sh, size=1794, modificationTime=1688102390403, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=plugins/metrics-jmx/flink-metrics-jmx-1.16.0.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/plugins/metrics-jmx/flink-metrics-jmx-1.16.0.jar, size=18250, modificationTime=1688102390422, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=plugins/metrics-influx/flink-metrics-influxdb-1.16.0.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/plugins/metrics-influx/flink-metrics-influxdb-1.16.0.jar, size=984101, modificationTime=1688102390863, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=plugins/metrics-slf4j/flink-metrics-slf4j-1.16.0.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/plugins/metrics-slf4j/flink-metrics-slf4j-1.16.0.jar, size=9907, modificationTime=1688102390884, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=plugins/README.txt, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/plugins/README.txt, size=654, modificationTime=1688102390925, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=plugins/metrics-datadog/flink-metrics-datadog-1.16.0.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/plugins/metrics-datadog/flink-metrics-datadog-1.16.0.jar, size=552525, modificationTime=1688102390953, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=TopSpeedWindowing.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/TopSpeedWindowing.jar, size=16879, modificationTime=1688102390984, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=flink-conf.yaml, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/application_1687757067625_0009-flink-conf.yaml2916269951502410754.tmp, size=870, modificationTime=1688102392852, visibility=APPLICATION, type=FILE}"
     * export CLASSPATH=":TopSpeedWindowing.jar:lib/flink-cep-1.16.0.jar:lib/flink-connector-files-1.16.0.jar:lib/flink-csv-1.16.0.jar:lib/flink-json-1.16.0.jar:lib/flink-scala_2.12-1.16.0.jar:lib/flink-shaded-zookeeper-3.5.9.jar:lib/flink-sql-connector-mysql-cdc-2.3.0.jar:lib/flink-table-api-java-uber-1.16.0.jar:lib/flink-table-planner-loader-1.16.0.jar:lib/flink-table-runtime-1.16.0.jar:lib/log4j-1.2-api-2.17.1.jar:lib/log4j-api-2.17.1.jar:lib/log4j-core-2.17.1.jar:lib/log4j-slf4j-impl-2.17.1.jar:lib/paimon-flink-1.16-0.4-SNAPSHOT.jar:lib/paimon-flink-action-0.4.0-incubating.jar:flink-dist-1.16.0.jar:flink-conf.yaml::$HADOOP_CONF_DIR:$HADOOP_COMMON_HOME/share/hadoop/common/*:$HADOOP_COMMON_HOME/share/hadoop/common/lib/*:$HADOOP_HDFS_HOME/share/hadoop/hdfs/*:$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*:$HADOOP_YARN_HOME/share/hadoop/yarn/*:$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*"
     * export _FLINK_YARN_FILES="hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009"
     * export _FLINK_CLASSPATH=":TopSpeedWindowing.jar:lib/flink-cep-1.16.0.jar:lib/flink-connector-files-1.16.0.jar:lib/flink-csv-1.16.0.jar:lib/flink-json-1.16.0.jar:lib/flink-scala_2.12-1.16.0.jar:lib/flink-shaded-zookeeper-3.5.9.jar:lib/flink-sql-connector-mysql-cdc-2.3.0.jar:lib/flink-table-api-java-uber-1.16.0.jar:lib/flink-table-planner-loader-1.16.0.jar:lib/flink-table-runtime-1.16.0.jar:lib/log4j-1.2-api-2.17.1.jar:lib/log4j-api-2.17.1.jar:lib/log4j-core-2.17.1.jar:lib/log4j-slf4j-impl-2.17.1.jar:lib/paimon-flink-1.16-0.4-SNAPSHOT.jar:lib/paimon-flink-action-0.4.0-incubating.jar:flink-dist-1.16.0.jar:flink-conf.yaml:"
     * export _FLINK_DIST_JAR="YarnLocalResourceDescriptor{key=flink-dist-1.16.0.jar, path=hdfs://hadoop101:8020/user/hdfs/.flink/application_1687757067625_0009/flink-dist-1.16.0.jar, size=117102633, modificationTime=1688102392774, visibility=APPLICATION, type=FILE}"
     * echo "Setting up job resources"
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/37/TopSpeedWindowing.jar" "TopSpeedWindowing.jar"
     * mkdir -p lib
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/17/paimon-flink-action-0.4.0-incubating.jar" "lib/paimon-flink-action-0.4.0-incubating.jar"
     * mkdir -p lib
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/20/log4j-1.2-api-2.17.1.jar" "lib/log4j-1.2-api-2.17.1.jar"
     * mkdir -p lib
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/36/flink-scala_2.12-1.16.0.jar" "lib/flink-scala_2.12-1.16.0.jar"
     * mkdir -p lib
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/16/log4j-slf4j-impl-2.17.1.jar" "lib/log4j-slf4j-impl-2.17.1.jar"
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/25/application_1687757067625_0009-flink-conf.yaml2916269951502410754.tmp" "flink-conf.yaml"
     * mkdir -p lib
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/39/paimon-flink-1.16-0.4-SNAPSHOT.jar" "lib/paimon-flink-1.16-0.4-SNAPSHOT.jar"
     * mkdir -p plugins/metrics-prometheus
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/24/flink-metrics-prometheus-1.16.0.jar" "plugins/metrics-prometheus/flink-metrics-prometheus-1.16.0.jar"
     * mkdir -p lib
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/19/flink-connector-files-1.16.0.jar" "lib/flink-connector-files-1.16.0.jar"
     * mkdir -p plugins/metrics-jmx
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/11/flink-metrics-jmx-1.16.0.jar" "plugins/metrics-jmx/flink-metrics-jmx-1.16.0.jar"
     * mkdir -p plugins/external-resource-gpu
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/33/gpu-discovery-common.sh" "plugins/external-resource-gpu/gpu-discovery-common.sh"
     * mkdir -p plugins/metrics-influx
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/40/flink-metrics-influxdb-1.16.0.jar" "plugins/metrics-influx/flink-metrics-influxdb-1.16.0.jar"
     * mkdir -p lib
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/14/flink-cep-1.16.0.jar" "lib/flink-cep-1.16.0.jar"
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/31/flink-dist-1.16.0.jar" "flink-dist-1.16.0.jar"
     * mkdir -p plugins/metrics-graphite
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/18/flink-metrics-graphite-1.16.0.jar" "plugins/metrics-graphite/flink-metrics-graphite-1.16.0.jar"
     * mkdir -p lib
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/22/flink-csv-1.16.0.jar" "lib/flink-csv-1.16.0.jar"
     * mkdir -p plugins/metrics-datadog
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/15/flink-metrics-datadog-1.16.0.jar" "plugins/metrics-datadog/flink-metrics-datadog-1.16.0.jar"
     * mkdir -p plugins/external-resource-gpu
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/29/nvidia-gpu-discovery.sh" "plugins/external-resource-gpu/nvidia-gpu-discovery.sh"
     * mkdir -p lib
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/28/flink-table-api-java-uber-1.16.0.jar" "lib/flink-table-api-java-uber-1.16.0.jar"
     * mkdir -p lib
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/26/log4j-api-2.17.1.jar" "lib/log4j-api-2.17.1.jar"
     * mkdir -p lib
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/13/flink-sql-connector-mysql-cdc-2.3.0.jar" "lib/flink-sql-connector-mysql-cdc-2.3.0.jar"
     * mkdir -p plugins
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/34/README.txt" "plugins/README.txt"
     * mkdir -p lib
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/38/flink-table-planner-loader-1.16.0.jar" "lib/flink-table-planner-loader-1.16.0.jar"
     * mkdir -p lib
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/10/flink-table-runtime-1.16.0.jar" "lib/flink-table-runtime-1.16.0.jar"
     * mkdir -p lib
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/30/flink-shaded-zookeeper-3.5.9.jar" "lib/flink-shaded-zookeeper-3.5.9.jar"
     * mkdir -p lib
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/32/log4j-core-2.17.1.jar" "lib/log4j-core-2.17.1.jar"
     * mkdir -p plugins/external-resource-gpu
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/27/flink-external-resource-gpu-1.16.0.jar" "plugins/external-resource-gpu/flink-external-resource-gpu-1.16.0.jar"
     * mkdir -p plugins/metrics-statsd
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/21/flink-metrics-statsd-1.16.0.jar" "plugins/metrics-statsd/flink-metrics-statsd-1.16.0.jar"
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/23/log4j.properties" "log4j.properties"
     * mkdir -p lib
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/35/flink-json-1.16.0.jar" "lib/flink-json-1.16.0.jar"
     * mkdir -p plugins/metrics-slf4j
     * ln -sf -- "/opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/12/flink-metrics-slf4j-1.16.0.jar" "plugins/metrics-slf4j/flink-metrics-slf4j-1.16.0.jar"
     * echo "Copying debugging information"
     * # Creating copy of launch script
     * cp "launch_container.sh" "/opt/app/hadoop-3.1.3/logs/userlogs/application_1687757067625_0009/container_1687757067625_0009_01_000001/launch_container.sh"
     * chmod 640 "/opt/app/hadoop-3.1.3/logs/userlogs/application_1687757067625_0009/container_1687757067625_0009_01_000001/launch_container.sh"
     * # Determining directory contents
     * echo "ls -l:" 1>"/opt/app/hadoop-3.1.3/logs/userlogs/application_1687757067625_0009/container_1687757067625_0009_01_000001/directory.info"
     * ls -l 1>>"/opt/app/hadoop-3.1.3/logs/userlogs/application_1687757067625_0009/container_1687757067625_0009_01_000001/directory.info"
     * echo "find -L . -maxdepth 5 -ls:" 1>>"/opt/app/hadoop-3.1.3/logs/userlogs/application_1687757067625_0009/container_1687757067625_0009_01_000001/directory.info"
     * find -L . -maxdepth 5 -ls 1>>"/opt/app/hadoop-3.1.3/logs/userlogs/application_1687757067625_0009/container_1687757067625_0009_01_000001/directory.info"
     * echo "broken symlinks(find -L . -maxdepth 5 -type l -ls):" 1>>"/opt/app/hadoop-3.1.3/logs/userlogs/application_1687757067625_0009/container_1687757067625_0009_01_000001/directory.info"
     * find -L . -maxdepth 5 -type l -ls 1>>"/opt/app/hadoop-3.1.3/logs/userlogs/application_1687757067625_0009/container_1687757067625_0009_01_000001/directory.info"
     * echo "Launching container"
     * <p>
     * exec /bin/bash -c "$JAVA_HOME/bin/java
     * -Xmx1073741824 -Xms1073741824 -XX:MaxMetaspaceSize=268435456
     * -Dlog.file="/opt/app/hadoop-3.1.3/logs/userlogs/application_1687757067625_0009/container_1687757067625_0009_01_000001/jobmanager.log"
     * -Dlog4j.configuration=file:log4j.properties
     * -Dlog4j.configurationFile=file:log4j.properties
     * org.apache.flink.yarn.entrypoint.YarnApplicationClusterEntryPoint
     * -D jobmanager.memory.off-heap.size=134217728b
     * -D jobmanager.memory.jvm-overhead.min=201326592b
     * -D jobmanager.memory.jvm-metaspace.size=268435456b
     * -D jobmanager.memory.heap.size=1073741824b
     * -D jobmanager.memory.jvm-overhead.max=201326592b
     * 1> /opt/app/hadoop-3.1.3/logs/userlogs/application_1687757067625_0009/container_1687757067625_0009_01_000001/jobmanager.out
     * 2> /opt/app/hadoop-3.1.3/logs/userlogs/application_1687757067625_0009/container_1687757067625_0009_01_000001/jobmanager.err"
     */
    public static void main(final String[] args) {
        // startup checks and logging
        // 启动检查和打印日志
        EnvironmentInformation.logEnvironmentInfo(
                LOG, YarnApplicationClusterEntryPoint.class.getSimpleName(), args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);
        // 获取系统环境变量
        Map<String, String> env = System.getenv();

        // 获取当前启动 AM 工作目录
        /**
         * [hdfs@hadoop102 ~]$ cd /opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/container_1687757067625_0009_01_000001
         * -rw-r--r--  1 hdfs hadoop    74 6月  30 13:19 container_tokens
         * -rwx------  1 hdfs hadoop   708 6月  30 13:19 default_container_executor_session.sh
         * -rwx------  1 hdfs hadoop   763 6月  30 13:19 default_container_executor.sh
         * lrwxrwxrwx  1 hdfs hadoop   177 6月  30 13:19 flink-conf.yaml -> /opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/25/application_1687757067625_0009-flink-conf.yaml2916269951502410754.tmp
         * lrwxrwxrwx  1 hdfs hadoop   129 6月  30 13:19 flink-dist-1.16.0.jar -> /opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/31/flink-dist-1.16.0.jar
         * -rwx------  1 hdfs hadoop 20851 6月  30 13:19 launch_container.sh
         * drwxr-xr-x  2 hdfs hadoop  4096 6月  30 13:19 lib
         * lrwxrwxrwx  1 hdfs hadoop   124 6月  30 13:19 log4j.properties -> /opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/23/log4j.properties
         * drwxr-xr-x 10 hdfs hadoop   210 6月  30 13:19 plugins
         * drwx--x---  2 hdfs hadoop     6 6月  30 13:19 tmp
         * lrwxrwxrwx  1 hdfs hadoop   129 6月  30 13:19 TopSpeedWindowing.jar -> /opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009/filecache/37/TopSpeedWindowing.jar
         */
        final String workingDirectory = env.get(ApplicationConstants.Environment.PWD.key());
        Preconditions.checkArgument(
                workingDirectory != null,
                "Working directory variable (%s) not set",
                ApplicationConstants.Environment.PWD.key());

        try {
            YarnEntrypointUtils.logYarnEnvironmentInformation(env, LOG);
        } catch (IOException e) {
            LOG.warn("Could not log YARN environment information.", e);
        }

        // 解析入口参数
        final Configuration dynamicParameters =
                ClusterEntrypointUtils.parseParametersOrExit(
                        args,
                        new DynamicParametersConfigurationParserFactory(),
                        YarnApplicationClusterEntryPoint.class);
        // 加载 flink-conf.yaml 配置参数+入口参数
        /**
         * flink-conf.yaml 一些内容如下
         * taskmanager.memory.process.size: 1728m
         * execution.savepoint-restore-mode: NO_CLAIM
         * jobmanager.execution.failover-strategy: region
         * high-availability.cluster-id: application_1687757067625_0009
         * jobmanager.rpc.address: localhost
         * execution.target: yarn-application
         * jobmanager.memory.process.size: 1600m
         * jobmanager.rpc.port: 6123
         * rest.bind-address: hadoop101
         * execution.savepoint.ignore-unclaimed-state: false
         * execution.attached: true
         * internal.cluster.execution-mode: NORMAL
         * $internal.application.program-args:
         * execution.shutdown-on-attached-exit: false
         * pipeline.jars: file:/opt/app/flink-1.16.0/./examples/streaming/TopSpeedWindowing.jar
         * parallelism.default: 1
         * taskmanager.numberOfTaskSlots: 2
         * rest.address: hadoop101
         * pipeline.classpaths:
         * $internal.deployment.config-dir: /opt/app/flink-1.16.0/conf
         * $internal.yarn.log-config-file: /opt/app/flink-1.16.0/conf/log4j.properties
         */
        /**
         * 2023-06-30 13:20:03,010 INFO  org.apache.flink.configuration.GlobalConfiguration           [] - Loading configuration property: taskmanager.memory.process.size, 1728m
         * 2023-06-30 13:20:03,010 INFO  org.apache.flink.configuration.GlobalConfiguration           [] - Loading configuration property: execution.savepoint-restore-mode, NO_CLAIM
         * 2023-06-30 13:20:03,010 INFO  org.apache.flink.configuration.GlobalConfiguration           [] - Loading configuration property: jobmanager.execution.failover-strategy, region
         * 2023-06-30 13:20:03,010 INFO  org.apache.flink.configuration.GlobalConfiguration           [] - Loading configuration property: high-availability.cluster-id, application_1687757067625_0009
         * 2023-06-30 13:20:03,010 INFO  org.apache.flink.configuration.GlobalConfiguration           [] - Loading configuration property: jobmanager.rpc.address, localhost
         * 2023-06-30 13:20:03,010 INFO  org.apache.flink.configuration.GlobalConfiguration           [] - Loading configuration property: execution.target, yarn-application
         * 2023-06-30 13:20:03,010 INFO  org.apache.flink.configuration.GlobalConfiguration           [] - Loading configuration property: jobmanager.memory.process.size, 1600m
         * 2023-06-30 13:20:03,010 INFO  org.apache.flink.configuration.GlobalConfiguration           [] - Loading configuration property: jobmanager.rpc.port, 6123
         * 2023-06-30 13:20:03,010 INFO  org.apache.flink.configuration.GlobalConfiguration           [] - Loading configuration property: rest.bind-address, hadoop101
         * 2023-06-30 13:20:03,010 INFO  org.apache.flink.configuration.GlobalConfiguration           [] - Loading configuration property: execution.savepoint.ignore-unclaimed-state, false
         * 2023-06-30 13:20:03,010 INFO  org.apache.flink.configuration.GlobalConfiguration           [] - Loading configuration property: execution.attached, true
         * 2023-06-30 13:20:03,010 INFO  org.apache.flink.configuration.GlobalConfiguration           [] - Loading configuration property: internal.cluster.execution-mode, NORMAL
         * 2023-06-30 13:20:03,010 INFO  org.apache.flink.configuration.GlobalConfiguration           [] - Loading configuration property: execution.shutdown-on-attached-exit, false
         * 2023-06-30 13:20:03,010 INFO  org.apache.flink.configuration.GlobalConfiguration           [] - Loading configuration property: pipeline.jars, file:/opt/app/flink-1.16.0/./examples/streaming/TopSpeedWindowing.jar
         * 2023-06-30 13:20:03,010 INFO  org.apache.flink.configuration.GlobalConfiguration           [] - Loading configuration property: parallelism.default, 1
         * 2023-06-30 13:20:03,011 INFO  org.apache.flink.configuration.GlobalConfiguration           [] - Loading configuration property: taskmanager.numberOfTaskSlots, 2
         * 2023-06-30 13:20:03,011 INFO  org.apache.flink.configuration.GlobalConfiguration           [] - Loading configuration property: rest.address, hadoop101
         * 2023-06-30 13:20:03,011 INFO  org.apache.flink.configuration.GlobalConfiguration           [] - Loading configuration property: $internal.deployment.config-dir, /opt/app/flink-1.16.0/conf
         * 2023-06-30 13:20:03,011 INFO  org.apache.flink.configuration.GlobalConfiguration           [] - Loading configuration property: $internal.yarn.log-config-file, /opt/app/flink-1.16.0/conf/log4j.properties
         * 2023-06-30 13:20:03,011 INFO  org.apache.flink.configuration.GlobalConfiguration           [] - Loading dynamic configuration property: jobmanager.memory.off-heap.size, 134217728b
         * 2023-06-30 13:20:03,011 INFO  org.apache.flink.configuration.GlobalConfiguration           [] - Loading dynamic configuration property: jobmanager.memory.jvm-overhead.min, 201326592b
         * 2023-06-30 13:20:03,011 INFO  org.apache.flink.configuration.GlobalConfiguration           [] - Loading dynamic configuration property: jobmanager.memory.jvm-metaspace.size, 268435456b
         * 2023-06-30 13:20:03,011 INFO  org.apache.flink.configuration.GlobalConfiguration           [] - Loading dynamic configuration property: jobmanager.memory.heap.size, 1073741824b
         * 2023-06-30 13:20:03,011 INFO  org.apache.flink.configuration.GlobalConfiguration           [] - Loading dynamic configuration property: jobmanager.memory.jvm-overhead.max, 201326592b
         * 2023-06-30 13:20:03,118 WARN  org.apache.flink.configuration.Configuration                 [] - Config uses deprecated configuration key 'web.port' instead of proper key 'rest.bind-port'
         * 2023-06-30 13:20:03,217 INFO  org.apache.flink.runtime.clusterframework.BootstrapTools     [] - Setting directories for temporary files to: /opt/app/hadoop-3.1.3/data/nm-local-dir/usercache/hdfs/appcache/application_1687757067625_0009
         */
        final Configuration configuration =
                YarnEntrypointUtils.loadConfiguration(workingDirectory, dynamicParameters, env);

        PackagedProgram program = null;
        try {
            // 解析程序任务入口主类和程序入口参数
            program = getPackagedProgram(configuration);
        } catch (Exception e) {
            LOG.error("Could not create application program.", e);
            System.exit(1);
        }

        try {
            // 配置程序执行参数
            configureExecution(configuration, program);
        } catch (Exception e) {
            LOG.error("Could not apply application configuration.", e);
            System.exit(1);
        }

        // 创建 YarnApplicationClusterEntryPoint
        YarnApplicationClusterEntryPoint yarnApplicationClusterEntrypoint =
                new YarnApplicationClusterEntryPoint(configuration, program);

        // 运行 YarnApplicationClusterEntryPoint
        ClusterEntrypoint.runClusterEntrypoint(yarnApplicationClusterEntrypoint);
    }

    private static PackagedProgram getPackagedProgram(final Configuration configuration)
            throws FlinkException {

        // 解析程序入口主类和程序入口参数
        final ApplicationConfiguration applicationConfiguration =
                ApplicationConfiguration.fromConfiguration(configuration);

        final PackagedProgramRetriever programRetriever =
                getPackagedProgramRetriever(
                        configuration,
                        applicationConfiguration.getProgramArguments(),
                        applicationConfiguration.getApplicationClassName());
        return programRetriever.getPackagedProgram();
    }

    private static PackagedProgramRetriever getPackagedProgramRetriever(
            final Configuration configuration,
            final String[] programArguments,
            @Nullable final String jobClassName)
            throws FlinkException {

        final File userLibDir = YarnEntrypointUtils.getUsrLibDir(configuration).orElse(null);

        // No need to do pipelineJars validation if it is a PyFlink job.
        if (!(PackagedProgramUtils.isPython(jobClassName)
                || PackagedProgramUtils.isPython(programArguments))) {
            final File userApplicationJar = getUserApplicationJar(userLibDir, configuration);
            return DefaultPackagedProgramRetriever.create(
                    userLibDir, userApplicationJar, jobClassName, programArguments, configuration);
        }

        return DefaultPackagedProgramRetriever.create(
                userLibDir, jobClassName, programArguments, configuration);
    }

    private static File getUserApplicationJar(
            final File userLibDir, final Configuration configuration) {
        final List<File> pipelineJars =
                configuration.get(PipelineOptions.JARS).stream()
                        .map(uri -> new File(userLibDir, new Path(uri).getName()))
                        .collect(Collectors.toList());

        Preconditions.checkArgument(pipelineJars.size() == 1, "Should only have one jar");
        return pipelineJars.get(0);
    }
}
