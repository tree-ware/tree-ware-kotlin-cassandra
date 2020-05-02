package org.tree_ware.cassandra.schema.map

import org.tree_ware.schema.core.RootSchema
import org.tree_ware.schema.core.Schema

interface SchemaMap {
    val schema: Schema

    val rootMap: RootSchemaMap
    val entityMaps: List<EntitySchemaMap>
}

interface RootSchemaMap {
    val rootSchema: RootSchema

    val keyspaceName: String
}

interface EntitySchemaMap {
    val pathKeyMaps: List<EntityKeysSchemaMap>

    /**
     * Introduces a partition-key called `partition_id` if value is non-zero.
     * The non-zero value is the number of values for `partition_id`.
     */
    val usePartitionId: Int

    val tableName: String
}

interface EntityKeysSchemaMap {
    val name: String
    val keyFieldMaps: List<KeyFieldSchemaMap>
}

interface KeyFieldSchemaMap {
    val name: String
    val type: KeyType
}

enum class KeyType {
    PARTITION,
    CLUSTERING
}
