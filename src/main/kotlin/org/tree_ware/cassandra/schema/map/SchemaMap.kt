package org.tree_ware.cassandra.schema.map

import org.tree_ware.schema.core.*

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
     * Introduces a synthetic-partition-key called `partition_id_` if value is non-zero.
     * The non-zero value is the number of values for `partition_id`.
     */
    val usePartitionId: Int

    val tableName: String

    val entityPathSchema: EntityPathSchema
}

interface EntityKeysSchemaMap {
    val name: String
    val keyFieldMaps: List<KeyFieldSchemaMap>

    val entitySchema: EntitySchema
}

interface KeyFieldSchemaMap {
    val name: String
    val type: KeyType

    val fieldSchema: FieldSchema
}

enum class KeyType {
    PARTITION,
    CLUSTERING
}
