/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.orchestration.db.direct_load_table

import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.orchestration.db.TableName
import io.airbyte.cdk.load.state.StreamProcessingFailed
import io.airbyte.cdk.load.write.StreamLoader
import io.airbyte.cdk.load.write.StreamStateStore

class DirectLoadTableAppendStreamLoader(
    override val stream: DestinationStream,
    private val initialStatus: DirectLoadInitialStatus,
    private val realTableName: TableName,
    private val tempTableName: TableName,
    private val tableOperations: DirectLoadTableOperations,
    private val streamStateStore: StreamStateStore<DirectLoadTableExecutionConfig>,
) : StreamLoader {
    override suspend fun start() {
        tableOperations.ensureSchemaMatches(stream, realTableName)
        if (initialStatus.tempTable != null) {
            tableOperations.ensureSchemaMatches(stream, tempTableName)
            tableOperations.copyTable(
                sourceTableName = tempTableName,
                targetTableName = realTableName
            )
            tableOperations.dropTable(tempTableName)
        }
        streamStateStore.put(stream.descriptor, DirectLoadTableExecutionConfig(realTableName))
    }

    override suspend fun close(hadNonzeroRecords: Boolean, streamFailure: StreamProcessingFailed?) {
        // do nothing
    }
}

class DirectLoadTableDedupStreamLoader(
    override val stream: DestinationStream,
    private val initialStatus: DirectLoadInitialStatus,
    private val realTableName: TableName,
    private val tempTableName: TableName,
    private val tableOperations: DirectLoadTableOperations,
    private val streamStateStore: StreamStateStore<DirectLoadTableExecutionConfig>,
) : StreamLoader {
    override suspend fun start() {
        if (initialStatus.tempTable != null) {
            tableOperations.ensureSchemaMatches(stream, tempTableName)
        } else {
            tableOperations.createTable(stream, tempTableName, replace = true)
        }
        streamStateStore.put(stream.descriptor, DirectLoadTableExecutionConfig(tempTableName))
    }

    override suspend fun close(hadNonzeroRecords: Boolean, streamFailure: StreamProcessingFailed?) {
        tableOperations.ensureSchemaMatches(stream, realTableName)
        tableOperations.upsertTable(
            sourceTableName = tempTableName,
            targetTableName = realTableName,
        )
        tableOperations.dropTable(tempTableName)
    }
}

class DirectLoadTableAppendTruncateStreamLoader(
    override val stream: DestinationStream,
    private val initialStatus: DirectLoadInitialStatus,
    private val realTableName: TableName,
    private val tempTableName: TableName,
    private val tableOperations: DirectLoadTableOperations,
    private val streamStateStore: StreamStateStore<DirectLoadTableExecutionConfig>,
) : StreamLoader {
    // can't use lateinit because of weird kotlin reasons.
    // this field is always overwritten in start().
    private var writingToTempTable: Boolean = false

    override suspend fun start() {
        if (initialStatus.tempTable != null) {
            if (
                initialStatus.tempTable.isEmpty ||
                    tableOperations.getGenerationId(tempTableName) < stream.minimumGenerationId
            ) {
                tableOperations.createTable(stream, tempTableName, replace = true)
            } else {
                tableOperations.ensureSchemaMatches(stream, tempTableName)
            }
            writingToTempTable = true
            streamStateStore.put(stream.descriptor, DirectLoadTableExecutionConfig(tempTableName))
        } else {
            if (initialStatus.realTable == null) {
                tableOperations.createTable(stream, realTableName, replace = true)
                writingToTempTable = false
            } else if (initialStatus.realTable.isEmpty) {
                tableOperations.ensureSchemaMatches(stream, realTableName)
                writingToTempTable = false
            } else if (
                tableOperations.getGenerationId(realTableName) < stream.minimumGenerationId
            ) {
                tableOperations.createTable(stream, tempTableName, replace = true)
                writingToTempTable = true
            } else {
                tableOperations.ensureSchemaMatches(stream, realTableName)
                writingToTempTable = false
            }
        }

        if (writingToTempTable) {
            streamStateStore.put(stream.descriptor, DirectLoadTableExecutionConfig(tempTableName))
        } else {
            streamStateStore.put(stream.descriptor, DirectLoadTableExecutionConfig(realTableName))
        }
    }

    override suspend fun close(hadNonzeroRecords: Boolean, streamFailure: StreamProcessingFailed?) {
        if (streamFailure == null && writingToTempTable) {
            tableOperations.overwriteTable(
                sourceTableName = tempTableName,
                targetTableName = realTableName
            )
            tableOperations.dropTable(tempTableName)
        }
    }
}

class DirectLoadTableDedupTruncateStreamLoader(
    override val stream: DestinationStream,
    private val initialStatus: DirectLoadInitialStatus,
    private val realTableName: TableName,
    private val tempTableName: TableName,
    private val tableOperations: DirectLoadTableOperations,
    private val streamStateStore: StreamStateStore<DirectLoadTableExecutionConfig>,
) : StreamLoader {
    // can't use lateinit because of weird kotlin reasons.
    // this field is always overwritten in start().
    private var finalTableKnownToBeWrongGeneration: Boolean = false

    override suspend fun start() {
        if (initialStatus.tempTable != null) {
            if (
                initialStatus.tempTable.isEmpty ||
                    tableOperations.getGenerationId(tempTableName) < stream.minimumGenerationId
            ) {
                tableOperations.createTable(stream, tempTableName, replace = true)
            } else {
                tableOperations.ensureSchemaMatches(stream, tempTableName)
            }
            finalTableKnownToBeWrongGeneration = true
        } else {
            tableOperations.createTable(stream, tempTableName, replace = true)
            finalTableKnownToBeWrongGeneration = false
        }
        streamStateStore.put(stream.descriptor, DirectLoadTableExecutionConfig(tempTableName))
    }

    override suspend fun close(hadNonzeroRecords: Boolean, streamFailure: StreamProcessingFailed?) {
        if (!finalTableKnownToBeWrongGeneration) {
            if (initialStatus.realTable != null && !initialStatus.realTable.isEmpty) {
                if (tableOperations.getGenerationId(realTableName) < stream.minimumGenerationId) {
                    tableOperations.upsertTable(
                        sourceTableName = tempTableName,
                        targetTableName = realTableName
                    )
                    tableOperations.dropTable(tempTableName)
                    return
                }
            }
        }

        if (streamFailure == null) {
            val tempTempTable = tempTableName.asTempTable()
            tableOperations.createTable(stream, tempTempTable, replace = true)
            tableOperations.upsertTable(
                sourceTableName = tempTableName,
                targetTableName = tempTempTable
            )
            tableOperations.overwriteTable(
                sourceTableName = tempTempTable,
                targetTableName = realTableName
            )
            tableOperations.dropTable(tempTableName)
        }
    }
}
