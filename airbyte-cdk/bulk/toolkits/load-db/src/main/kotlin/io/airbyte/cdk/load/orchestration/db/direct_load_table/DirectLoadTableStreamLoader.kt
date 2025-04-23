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
    private val nativeTableOperations: DirectLoadTableNativeOperations,
    private val sqlTableOperations: DirectLoadTableSqlOperations,
    private val streamStateStore: StreamStateStore<DirectLoadTableExecutionConfig>,
) : StreamLoader {
    override suspend fun start() {
        nativeTableOperations.ensureSchemaMatches(stream, realTableName)
        if (initialStatus.tempTable != null) {
            nativeTableOperations.ensureSchemaMatches(stream, tempTableName)
            sqlTableOperations.copyTable(
                sourceTableName = tempTableName,
                targetTableName = realTableName
            )
            sqlTableOperations.dropTable(tempTableName)
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
    private val nativeTableOperations: DirectLoadTableNativeOperations,
    private val sqlTableOperations: DirectLoadTableSqlOperations,
    private val streamStateStore: StreamStateStore<DirectLoadTableExecutionConfig>,
) : StreamLoader {
    override suspend fun start() {
        if (initialStatus.tempTable != null) {
            nativeTableOperations.ensureSchemaMatches(stream, tempTableName)
        } else {
            sqlTableOperations.createTable(stream, tempTableName, replace = true)
        }
        streamStateStore.put(stream.descriptor, DirectLoadTableExecutionConfig(tempTableName))
    }

    override suspend fun close(hadNonzeroRecords: Boolean, streamFailure: StreamProcessingFailed?) {
        nativeTableOperations.ensureSchemaMatches(stream, realTableName)
        sqlTableOperations.upsertTable(
            sourceTableName = tempTableName,
            targetTableName = realTableName,
        )
        sqlTableOperations.dropTable(tempTableName)
    }
}

class DirectLoadTableAppendTruncateStreamLoader(
    override val stream: DestinationStream,
    private val initialStatus: DirectLoadInitialStatus,
    private val realTableName: TableName,
    private val tempTableName: TableName,
    private val nativeTableOperations: DirectLoadTableNativeOperations,
    private val sqlTableOperations: DirectLoadTableSqlOperations,
    private val streamStateStore: StreamStateStore<DirectLoadTableExecutionConfig>,
) : StreamLoader {
    // can't use lateinit because of weird kotlin reasons.
    // this field is always overwritten in start().
    private var writingToTempTable: Boolean = false

    override suspend fun start() {
        if (initialStatus.tempTable != null) {
            if (
                initialStatus.tempTable.isEmpty ||
                    nativeTableOperations.getGenerationId(tempTableName) >=
                        stream.minimumGenerationId
            ) {
                nativeTableOperations.ensureSchemaMatches(stream, tempTableName)
            } else {
                sqlTableOperations.createTable(stream, tempTableName, replace = true)
            }
            writingToTempTable = true
            streamStateStore.put(stream.descriptor, DirectLoadTableExecutionConfig(tempTableName))
        } else {
            if (initialStatus.realTable == null) {
                sqlTableOperations.createTable(stream, realTableName, replace = true)
                writingToTempTable = false
            } else if (
                initialStatus.realTable.isEmpty ||
                    nativeTableOperations.getGenerationId(realTableName) >=
                        stream.minimumGenerationId
            ) {
                nativeTableOperations.ensureSchemaMatches(stream, realTableName)
                writingToTempTable = false
            } else {
                sqlTableOperations.createTable(stream, tempTableName, replace = true)
                writingToTempTable = true
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
            sqlTableOperations.overwriteTable(
                sourceTableName = tempTableName,
                targetTableName = realTableName
            )
        }
    }
}

class DirectLoadTableDedupTruncateStreamLoader(
    override val stream: DestinationStream,
    private val initialStatus: DirectLoadInitialStatus,
    private val realTableName: TableName,
    private val tempTableName: TableName,
    private val nativeTableOperations: DirectLoadTableNativeOperations,
    private val sqlTableOperations: DirectLoadTableSqlOperations,
    private val streamStateStore: StreamStateStore<DirectLoadTableExecutionConfig>,
) : StreamLoader {
    // can't use lateinit because of weird kotlin reasons.
    // this field is always overwritten in start().
    private var finalTableMaybeCorrectGeneration: Boolean = false

    override suspend fun start() {
        if (initialStatus.tempTable != null) {
            if (
                initialStatus.tempTable.isEmpty ||
                    nativeTableOperations.getGenerationId(tempTableName) >=
                        stream.minimumGenerationId
            ) {
                nativeTableOperations.ensureSchemaMatches(stream, tempTableName)
            } else {
                sqlTableOperations.createTable(stream, tempTableName, replace = true)
            }
            finalTableMaybeCorrectGeneration = false
        } else {
            sqlTableOperations.createTable(stream, tempTableName, replace = true)
            finalTableMaybeCorrectGeneration = true
        }
        streamStateStore.put(stream.descriptor, DirectLoadTableExecutionConfig(tempTableName))
    }

    override suspend fun close(hadNonzeroRecords: Boolean, streamFailure: StreamProcessingFailed?) {
        if (finalTableMaybeCorrectGeneration) {
            if (initialStatus.realTable == null) {
                sqlTableOperations.createTable(stream, realTableName, replace = true)
                sqlTableOperations.upsertTable(
                    sourceTableName = tempTableName,
                    targetTableName = realTableName
                )
                sqlTableOperations.dropTable(tempTableName)
                return
            } else if (
                initialStatus.realTable.isEmpty ||
                    nativeTableOperations.getGenerationId(realTableName) >=
                        stream.minimumGenerationId
            ) {
                sqlTableOperations.upsertTable(
                    sourceTableName = tempTableName,
                    targetTableName = realTableName
                )
                sqlTableOperations.dropTable(tempTableName)
                return
            }
        }

        if (streamFailure == null) {
            val tempTempTable = tempTableName.asTempTable()
            sqlTableOperations.createTable(stream, tempTempTable, replace = true)
            sqlTableOperations.upsertTable(
                sourceTableName = tempTableName,
                targetTableName = tempTempTable
            )
            sqlTableOperations.overwriteTable(
                sourceTableName = tempTempTable,
                targetTableName = realTableName
            )
        }
    }
}
