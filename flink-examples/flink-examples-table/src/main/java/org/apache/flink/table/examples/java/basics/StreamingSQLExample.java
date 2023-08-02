package org.apache.flink.table.examples.java.basics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class StreamingSQLExample {

    public static void main(String[] args) {

        Configuration configuration = new Configuration();

        // 1 初始化环境设置
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .withBuiltInCatalogName("default_catalog")
                .withBuiltInDatabaseName("default_database")
                .withConfiguration(configuration)
                .build();

        // 2 创建表环境
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 3 创建表 (也就将表信息缓存起来 GenericInMemoryCatalog)
        tableEnv.executeSql("CREATE TABLE socket_info (\n" +
                "                `user` STRING,\n" +
                "                `url` STRING,\n" +
                "                `ts` BIGINT\n" +
                "            ) WITH (\n" +
                "                'connector' = 'socket',\n" +
                "                'hostname' = 'hadoop',\n" +
                "                'port' = '10000',\n" +
                "                'format' = 'csv'\n" +
                ")");
        tableEnv.executeSql("CREATE TABLE print_info (\n" +
                "                `user` STRING,\n" +
                "                `url` STRING\n" +
                "            ) WITH (\n" +
                "                'connector' = 'print'\n" +
                ")");

        // 4 执行插入
        tableEnv.executeSql("insert into print_info SELECT `user`,`url` from socket_info");

    }

}
