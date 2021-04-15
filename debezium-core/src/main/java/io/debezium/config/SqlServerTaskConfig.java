/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import org.apache.kafka.common.config.ConfigDef;

public class SqlServerTaskConfig {

    public static final Field TASK_DATABASE_NAMES = Field.create("task.database.dbnames")
            .withDisplayName("Task Database Names")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("Comma-separated list of the names of the databases assigned to a task");

    public static Field.Set ALL_FIELDS = Field.setOf(TASK_DATABASE_NAMES);

    private final String[] databaseNames;

    public SqlServerTaskConfig(Configuration config) {
        databaseNames = config.getString(TASK_DATABASE_NAMES).split(",");
    }

    public String[] getDatabaseNames() {
        return databaseNames;
    }
}
