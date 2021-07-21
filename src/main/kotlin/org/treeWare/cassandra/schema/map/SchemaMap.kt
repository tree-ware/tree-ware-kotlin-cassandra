package org.treeWare.cassandra.schema.map

import org.treeWare.schema.core.*

interface SchemaMap {
    val schema: Schema

    val root: RootSchemaMap
    val entityPaths: List<EntityPathSchemaMap>
}

interface RootSchemaMap {
    val schema: RootSchema

    val keyspaceName: String
}

interface EntityPathSchemaMap {
    val pathEntities: List<PathEntitySchemaMap>

    /** Number of values for the synthetic-partition-key called `part_id_`. */
    val syntheticPartIdSize: Int

    val tableName: String

    val schema: EntityPathSchema
}

interface PathEntitySchemaMap {
    val name: String
    val keys: List<KeyFieldSchemaMap>

    val schema: EntitySchema
}

interface KeyFieldSchemaMap {
    val name: String
    val type: KeyType

    val nestedKeyParentName: String?
    val schema: FieldSchema
}

enum class KeyType {
    PARTITION,
    CLUSTERING
}
