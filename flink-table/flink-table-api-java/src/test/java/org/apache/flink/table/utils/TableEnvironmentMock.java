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

package org.apache.flink.table.utils;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.resource.ResourceManager;

import java.net.URL;

/** Mocking {@link TableEnvironment} for tests. */
public class TableEnvironmentMock extends TableEnvironmentImpl {

    public final CatalogManager catalogManager;

    public final ExecutorMock executor;

    public final FunctionCatalog functionCatalog;

    public final PlannerMock planner;

    protected TableEnvironmentMock(
            CatalogManager catalogManager,
            ModuleManager moduleManager,
            ResourceManager userResourceManager,
            TableConfig tableConfig,
            ExecutorMock executor,
            FunctionCatalog functionCatalog,
            PlannerMock planner,
            boolean isStreamingMode) {
        super(
                catalogManager,
                moduleManager,
                userResourceManager,
                tableConfig,
                executor,
                functionCatalog,
                planner,
                isStreamingMode);
        this.catalogManager = catalogManager;
        this.executor = executor;
        this.functionCatalog = functionCatalog;
        this.planner = planner;
    }

    public static TableEnvironmentMock getStreamingInstance() {
        return getInstance(true);
    }

    public static TableEnvironmentMock getBatchInstance() {
        return getInstance(false);
    }

    private static TableEnvironmentMock getInstance(boolean isStreamingMode) {
        final TableConfig tableConfig = TableConfig.getDefault();
        final CatalogManager catalogManager = CatalogManagerMocks.createEmptyCatalogManager();
        final ModuleManager moduleManager = new ModuleManager();
        final ResourceManager resourceManager =
                ResourceManager.createResourceManager(
                        new URL[0],
                        Thread.currentThread().getContextClassLoader(),
                        tableConfig.getConfiguration());

        return new TableEnvironmentMock(
                catalogManager,
                moduleManager,
                resourceManager,
                tableConfig,
                createExecutor(),
                createFunctionCatalog(tableConfig, resourceManager, catalogManager, moduleManager),
                createPlanner(),
                isStreamingMode);
    }

    private static ExecutorMock createExecutor() {
        return new ExecutorMock();
    }

    private static FunctionCatalog createFunctionCatalog(
            ReadableConfig config,
            ResourceManager resourceManager,
            CatalogManager catalogManager,
            ModuleManager moduleManager) {
        return new FunctionCatalog(config, resourceManager, catalogManager, moduleManager);
    }

    private static PlannerMock createPlanner() {
        return new PlannerMock();
    }
}
