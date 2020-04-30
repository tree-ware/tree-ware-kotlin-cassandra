package org.tree_ware.cassandra.schema.map

import org.tree_ware.schema.core.RootSchema
import org.tree_ware.schema.core.Schema

class MutableSchemaMap(
    override val schema: Schema,
    keyspaceName: String? = null,
    override val entities: List<MutableEntitySchemaMap>
) : SchemaMap {
    override val root = MutableRootSchemaMap(schema.root, keyspaceName)
}

class MutableRootSchemaMap(
    override val rootSchema: RootSchema,
    keyspaceName: String? = null
) : RootSchemaMap {
    override val keyspaceName = keyspaceName ?: rootSchema.name
}

class MutableEntitySchemaMap(
    override val pathKeys: List<MutableEntityKeysSchemaMap>,
    override val usePartitionId: Int = 0,
    tableName: String? = null
) : EntitySchemaMap {
    override val tableName = tableName ?: getGeneratedTableName()

    private fun getGeneratedTableName(): String = "TODO"
}

class MutableEntityKeysSchemaMap(
    override val name: String,
    override val keys: List<MutableKeyFieldSchemaMap>
) : EntityKeysSchemaMap

class MutableKeyFieldSchemaMap(override val name: String, override val type: KeyType) : KeyFieldSchemaMap
