/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.SqlServerTaskConfig;
import io.debezium.connector.common.TaskPartition;

public class SqlServerTaskPartition implements TaskPartition {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerTaskPartition.class);
    private static final String SERVER_PARTITION_KEY = "server";
    private static final String DATABASE_PARTITION_KEY = "database";

    private final String serverName;
    private final String databaseName;

    public SqlServerTaskPartition(String serverName, String databaseName) {
        this.serverName = serverName;
        this.databaseName = databaseName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        Map<String, String> partition = new HashMap<>();
        partition.put(SERVER_PARTITION_KEY, serverName);
        partition.put(DATABASE_PARTITION_KEY, databaseName);

        return partition;
    }

    String getDatabaseName() {
        return databaseName;
    }

    static class Provider implements TaskPartition.Provider<SqlServerTaskPartition> {
        private final SqlServerConnectorConfig connectorConfig;
        private final SqlServerTaskConfig taskConfig;
        private final SqlServerConnection connection;

        Provider(SqlServerConnectorConfig connectorConfig, SqlServerTaskConfig taskConfig, SqlServerConnection connection) {
            this.connectorConfig = connectorConfig;
            this.taskConfig = taskConfig;
            this.connection = connection;
        }

        @Override
        public Collection<SqlServerTaskPartition> getPartitions() {
            String serverName = connectorConfig.getLogicalName();

            return Arrays.stream(taskConfig.getDatabaseNames())
                    .map(databaseName -> {
                        try {
                            return connection.retrieveRealDatabaseName(databaseName);
                        }
                        catch (RuntimeException e) {
                            LOGGER.warn("Couldn't obtain real name for database {}", databaseName);
                            return "";
                        }
                    })
                    .filter(realDatabaseName -> !realDatabaseName.isEmpty())
                    .map(realDatabaseName -> new SqlServerTaskPartition(serverName, realDatabaseName))
                    .collect(Collectors.toList());
        }
    }
}
