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
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

/**
 * The {@link SocketDynamicTableFactory} translates the catalog table to a table source.
 *
 * <p>Because the table source requires a decoding format, we are discovering the format using the
 * provided {@link FactoryUtil} for convenience.
 */
public final class SocketDynamicTableFactory implements DynamicTableSourceFactory {

    // define all options statically
    // 1 定义 socket connector 静态选项
    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname").stringType().noDefaultValue();
    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port").intType().noDefaultValue();
    public static final ConfigOption<Integer> BYTE_DELIMITER =
            ConfigOptions.key("byte-delimiter").intType().defaultValue(10); // corresponds to '\n'

    @Override
    public String factoryIdentifier() {
        // 2 socket connector 标识符
        return "socket"; // used for matching to `connector = '...'`
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        // 3 定义 socket connector 必选项参数
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(PORT);
        options.add(FactoryUtil.FORMAT); // use pre-defined option for format
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        // 4 定义 socket connector 可选项参数
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BYTE_DELIMITER);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // either implement your custom validation logic here ...
        // or use the provided helper utility
        // 5 创建校验逻辑工具 TableFactoryHelper
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        // discover a suitable decoding format
        // 6 寻找合适的解码器
        // 比如 'format' = 'csv' -> CsvFormatFactory
        // 比如 'format' = 'json' -> JsonFormatFactory ->
        // 比如 'format' = 'changelog-csv' -> ChangelogCsvFormatFactory -> ChangelogCsvFormat
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(
                        DeserializationFormatFactory.class, FactoryUtil.FORMAT);

        // validate all options
        // 7 校验 options
        helper.validate();

        // get the validated options
        // 8 获取合法的选项
        final ReadableConfig options = helper.getOptions();
        // 8.1 获取 hostname 选项
        final String hostname = options.get(HOSTNAME);
        // 8.2 获取 port 选项
        final int port = options.get(PORT);
        // 8.3 获取 byte-delimiter 选项 默认 \n 换行符
        final byte byteDelimiter = (byte) (int) options.get(BYTE_DELIMITER);

        // derive the produced data type (excluding computed columns) from the catalog table
        // 9 获取数据类型 比如 name STRING, score INT
        final DataType producedDataType =
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        // create and return dynamic table source
        // 10 创建动态 source 表
        return new SocketDynamicTableSource(
                hostname, port, byteDelimiter, decodingFormat, producedDataType);
    }
}
