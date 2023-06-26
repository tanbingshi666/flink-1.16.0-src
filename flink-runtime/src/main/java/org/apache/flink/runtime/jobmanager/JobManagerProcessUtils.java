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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.util.config.memory.CommonProcessMemorySpec;
import org.apache.flink.runtime.util.config.memory.JvmMetaspaceAndOverheadOptions;
import org.apache.flink.runtime.util.config.memory.LegacyMemoryOptions;
import org.apache.flink.runtime.util.config.memory.MemoryBackwardsCompatibilityUtils;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryOptions;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils;
import org.apache.flink.runtime.util.config.memory.jobmanager.JobManagerFlinkMemory;
import org.apache.flink.runtime.util.config.memory.jobmanager.JobManagerFlinkMemoryUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** JobManager utils to calculate {@link JobManagerProcessSpec} and JVM args. */
public class JobManagerProcessUtils {

    // JM 进程内存 默认 1600M = 1024 + 128 + 256 + 192
    static final ProcessMemoryOptions JM_PROCESS_MEMORY_OPTIONS =
            new ProcessMemoryOptions(
                    // JM JVM 堆内存
                    // key = jobmanager.memory.heap.size 默认值 1024MB 最小值 128MB
                    Collections.singletonList(JobManagerOptions.JVM_HEAP_MEMORY),
                    // JM Flink 内存 (JVM 堆内存 + 堆外内存[默认 128MB])
                    // key = jobmanager.memory.flink.size
                    JobManagerOptions.TOTAL_FLINK_MEMORY,
                    // JM 进程内存 ( Flink 内存 + JVM Metaspace + JVM Overhead)
                    // key = jobmanager.memory.process.size
                    JobManagerOptions.TOTAL_PROCESS_MEMORY,
                    // JM JVM 元空间以及Overhead
                    new JvmMetaspaceAndOverheadOptions(
                            // JVM 元空间内存
                            // key = jobmanager.memory.jvm-metaspace.size
                            // 默认 value = 256MB
                            JobManagerOptions.JVM_METASPACE,
                            // JVM Overhead 内存
                            // key = jobmanager.memory.jvm-overhead.min
                            // 默认 value = 192MB
                            JobManagerOptions.JVM_OVERHEAD_MIN,
                            // key = jobmanager.memory.jvm-overhead.max
                            // 默认 value = 1024MB
                            JobManagerOptions.JVM_OVERHEAD_MAX,
                            // JM 进程内存预留内存给 JVM Overhead
                            // key = jobmanager.memory.jvm-overhead.fraction
                            // 默认 value = 0.1f
                            JobManagerOptions.JVM_OVERHEAD_FRACTION));

    @SuppressWarnings("deprecation")
    static final LegacyMemoryOptions JM_LEGACY_HEAP_OPTIONS =
            new LegacyMemoryOptions(
                    "FLINK_JM_HEAP",
                    JobManagerOptions.JOB_MANAGER_HEAP_MEMORY,
                    JobManagerOptions.JOB_MANAGER_HEAP_MEMORY_MB);

    private static final ProcessMemoryUtils<JobManagerFlinkMemory> PROCESS_MEMORY_UTILS =
            new ProcessMemoryUtils<>(JM_PROCESS_MEMORY_OPTIONS, new JobManagerFlinkMemoryUtils());

    private static final MemoryBackwardsCompatibilityUtils LEGACY_MEMORY_UTILS =
            new MemoryBackwardsCompatibilityUtils(JM_LEGACY_HEAP_OPTIONS);

    private JobManagerProcessUtils() {
    }

    public static JobManagerProcessSpec processSpecFromConfigWithNewOptionToInterpretLegacyHeap(
            Configuration config, ConfigOption<MemorySize> newOptionToInterpretLegacyHeap) {
        try {
            // 计算 JM 进程内存
            return processSpecFromConfig(
                    // 如果在 flink-conf.yaml 配置老版本的 JM 内存设置 则需要映射成新版本的配置
                    // 一般情况下 直接返回 Configuration 即可
                    getConfigurationWithLegacyHeapSizeMappedToNewConfigOption(
                            config, newOptionToInterpretLegacyHeap));
        } catch (IllegalConfigurationException e) {
            throw new IllegalConfigurationException(
                    "JobManager memory configuration failed: " + e.getMessage(), e);
        }
    }

    static JobManagerProcessSpec processSpecFromConfig(Configuration config) {
        // 创建 JobManagerProcessSpec
        return createMemoryProcessSpec(
                // JM 内存处理
                PROCESS_MEMORY_UTILS.memoryProcessSpecFromConfig(config)
        );
    }

    private static JobManagerProcessSpec createMemoryProcessSpec(
            CommonProcessMemorySpec<JobManagerFlinkMemory> processMemory) {
        // 创建 JobManagerProcessSpec
        return new JobManagerProcessSpec(
                processMemory.getFlinkMemory(), processMemory.getJvmMetaspaceAndOverhead());
    }

    static Configuration getConfigurationWithLegacyHeapSizeMappedToNewConfigOption(
            Configuration configuration, ConfigOption<MemorySize> configOption) {
        return LEGACY_MEMORY_UTILS.getConfWithLegacyHeapSizeMappedToNewConfigOption(
                configuration, configOption);
    }

    @VisibleForTesting
    public static JobManagerProcessSpec createDefaultJobManagerProcessSpec(
            int totalProcessMemoryMb) {
        Configuration configuration = new Configuration();
        configuration.set(
                JobManagerOptions.TOTAL_PROCESS_MEMORY,
                MemorySize.ofMebiBytes(totalProcessMemoryMb));
        return processSpecFromConfig(configuration);
    }

    public static String generateJvmParametersStr(
            JobManagerProcessSpec processSpec, Configuration configuration) {
        return ProcessMemoryUtils.generateJvmParametersStr(
                processSpec,
                configuration.getBoolean(JobManagerOptions.JVM_DIRECT_MEMORY_LIMIT_ENABLED));
    }

    public static String generateDynamicConfigsStr(
            final JobManagerProcessSpec jobManagerProcessSpec) {
        final Map<String, String> config = new HashMap<>();

        config.put(
                JobManagerOptions.JVM_HEAP_MEMORY.key(),
                jobManagerProcessSpec.getJvmHeapMemorySize().getBytes() + "b");
        config.put(
                JobManagerOptions.OFF_HEAP_MEMORY.key(),
                jobManagerProcessSpec.getJvmDirectMemorySize().getBytes() + "b");

        config.put(
                JobManagerOptions.JVM_METASPACE.key(),
                jobManagerProcessSpec.getJvmMetaspaceSize().getBytes() + "b");
        config.put(
                JobManagerOptions.JVM_OVERHEAD_MIN.key(),
                jobManagerProcessSpec.getJvmOverheadSize().getBytes() + "b");
        config.put(
                JobManagerOptions.JVM_OVERHEAD_MAX.key(),
                jobManagerProcessSpec.getJvmOverheadSize().getBytes() + "b");

        return ConfigurationUtils.assembleDynamicConfigsStr(config);
    }
}
