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

package org.apache.flink.table.examples.java.connectors;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * The {@link ChangelogCsvFormatFactory} translates format-specific options to a format.
 *
 * <p>The {@link FactoryUtil} in {@link SocketDynamicTableFactory} takes care of adapting the option
 * keys accordingly and handles the prefixing like {@code changelog-csv.column-delimiter}.
 *
 * <p>Because this factory implements {@link DeserializationFormatFactory}, it could also be used
 * for other connectors that support deserialization formats such as the Kafka connector.
 */
public final class ChangelogCsvFormatFactory implements DeserializationFormatFactory {

    // define all options statically
    // 1 定义静态选项
    public static final ConfigOption<String> COLUMN_DELIMITER =
            ConfigOptions.key("column-delimiter").stringType().defaultValue("|");

    @Override
    public String factoryIdentifier() {
        // 2 changelog-svc 唯一标识符
        return "changelog-csv";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        // 3 必选项
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        // 4 可选项
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(COLUMN_DELIMITER);
        return options;
    }

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        // either implement your custom validation logic here ...
        // or use the provided helper method
        // 1 校验 options
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        // get the validated options
        // 2 获取选项 changelog-csv.column-delimiter 对应的值
        final String columnDelimiter = formatOptions.get(COLUMN_DELIMITER);

        // create and return the format
        // 3 创建 format
        return new ChangelogCsvFormat(columnDelimiter);
    }
}
