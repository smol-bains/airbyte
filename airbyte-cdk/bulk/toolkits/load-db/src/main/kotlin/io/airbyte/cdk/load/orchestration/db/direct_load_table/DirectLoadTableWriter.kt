/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.orchestration.db.direct_load_table

import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.orchestration.ColumnNameMapping
import io.airbyte.cdk.load.orchestration.DestinationHandler
import io.airbyte.cdk.load.orchestration.DestinationInitialStatusGatherer
import io.airbyte.cdk.load.orchestration.TableNames
import io.airbyte.cdk.load.write.DestinationWriter
import io.airbyte.cdk.load.write.StreamLoader

class DirectLoadTableWriter(
    private val names: Map<DestinationStream, Pair<TableNames, ColumnNameMapping>>,
    private val stateGatherer: DestinationInitialStatusGatherer<DirectLoadInitialStatus>,
    private val destinationHandler: DestinationHandler,
    private val tableOperations: DirectLoadTableOperations,
) : DestinationWriter {
    private lateinit var initialStatuses: Map<DestinationStream, DirectLoadInitialStatus>
    override suspend fun setup() {
        val namespaces =
            names.values.map { (tableNames, _) -> tableNames.finalTableName!!.namespace }.toSet()
        destinationHandler.createNamespaces(namespaces)

        initialStatuses = stateGatherer.gatherInitialStatus(names)
    }

    override fun createStreamLoader(stream: DestinationStream): StreamLoader {
        return DirectLoadTableStreamLoader(stream, tableOperations)
    }
}
