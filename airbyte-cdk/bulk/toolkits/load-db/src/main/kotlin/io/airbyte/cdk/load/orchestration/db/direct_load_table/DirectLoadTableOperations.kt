/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.orchestration.db.direct_load_table

import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.orchestration.db.DatabaseHandler
import io.airbyte.cdk.load.orchestration.db.TableName

class DirectLoadTableOperations(
    private val sqlGenerator: DirectLoadSqlGenerator,
    private val databaseHandler: DatabaseHandler,
) {
    fun createTable(
        stream: DestinationStream,
        tableName: TableName,
        replace: Boolean,
    ) {
        databaseHandler.execute(TODO())
    }

    fun ensureSchemaMatches(
        stream: DestinationStream,
        tableName: TableName,
    ) {
        // TODO we should figure out some reasonable abstraction for diffing existing+expected
        //  table schema
        TODO()
    }

    /**
     * Replace the targetTable with the sourceTable. This is typically something like
     * ```
     * DROP TABLE target;
     * ALTER TABLE source RENAME TO target;
     * ```
     */
    fun overwriteTable(
        sourceTableName: TableName,
        targetTableName: TableName,
    ) {
        TODO()
    }

    fun copyTable(
        sourceTableName: TableName,
        targetTableName: TableName,
    ) {
        TODO()
    }

    fun upsertTable(
        sourceTableName: TableName,
        targetTableName: TableName,
    ) {
        TODO()
    }

    /**
     * Return the generation ID of an arbitrary record from the table. May assume the table exists
     * and is nonempty.
     *
     * If an existing record has null generation, treat that record as belonging to generation 0.
     * These records predate the refreshes project.
     */
    fun getGenerationId(tableName: TableName): Long {
        TODO()
    }

    fun dropTable(tableName: TableName) {
        TODO()
    }
}
