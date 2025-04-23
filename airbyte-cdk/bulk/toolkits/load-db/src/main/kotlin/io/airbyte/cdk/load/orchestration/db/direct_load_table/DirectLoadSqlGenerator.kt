/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.orchestration.db.direct_load_table

import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.orchestration.db.Sql
import io.airbyte.cdk.load.orchestration.db.TableName

interface DirectLoadSqlGenerator {
    fun createTable(
        stream: DestinationStream,
        tableName: TableName,
        replace: Boolean,
    ): Sql

    /**
     * Replace the targetTable with the sourceTable. This is typically something like
     * ```sql
     * DROP TABLE target;
     * ALTER TABLE source RENAME TO target;
     * ```
     */
    fun overwriteTable(
        sourceTableName: TableName,
        targetTableName: TableName,
    ): Sql

    fun copyTable(
        sourceTableName: TableName,
        targetTableName: TableName,
    ): Sql

    fun upsertTable(
        sourceTableName: TableName,
        targetTableName: TableName,
    ): Sql

    fun dropTable(tableName: TableName): Sql
}
