/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.orchestration.direct_load_table

import io.airbyte.cdk.load.orchestration.DestinationInitialStatus

data class DirectLoadInitialStatus(
    val realTable: DirectLoadTableStatus?,
    val tempTable: DirectLoadTableStatus?,
) : DestinationInitialStatus

data class DirectLoadTableStatus(
    val isEmpty: Boolean,
)
