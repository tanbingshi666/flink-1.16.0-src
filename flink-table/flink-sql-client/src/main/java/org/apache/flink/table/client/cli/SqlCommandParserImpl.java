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

package org.apache.flink.table.client.cli;

import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.operations.Operation;

import java.util.Optional;

/** SqlCommandParserImpl wrappers an {@link Executor} supports parse a statement to an Operation. */
public class SqlCommandParserImpl implements SqlCommandParser {
    private final Executor executor;
    private final String sessionId;

    public SqlCommandParserImpl(Executor executor, String sessionId) {
        this.executor = executor;
        this.sessionId = sessionId;
    }

    @Override
    public Optional<Operation> parseCommand(String stmt) throws SqlParserException {
        // normalize
        // 1 一条 SQL 去空
        stmt = stmt.trim();
        // meet empty statement, e.g ";\n"
        if (stmt.isEmpty() || stmt.equals(";")) {
            return Optional.empty();
        }
        // 2 解析 SQL 成 Operation
        return Optional.ofNullable(executor.parseStatement(sessionId, stmt));
    }
}
