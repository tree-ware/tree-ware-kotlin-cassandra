package org.tree_ware.cassandra.schema.map

import org.tree_ware.schema.core.EntityPathSchema
import org.tree_ware.schema.core.RootSchema
import org.tree_ware.schema.core.Schema

class MutableSchemaMap(
    override val schema: Schema,
    keyspaceName: String? = null,
    override val entityMaps: List<MutableEntitySchemaMap>
) : SchemaMap {
    override val rootMap = MutableRootSchemaMap(schema.root, keyspaceName)
}

class MutableRootSchemaMap(
    override val rootSchema: RootSchema,
    keyspaceName: String? = null
) : RootSchemaMap {
    override val keyspaceName = "tw.$keyspaceName" ?: "tw.${rootSchema.name}"
}

class MutableEntitySchemaMap(
    override val pathKeyMaps: List<MutableEntityKeysSchemaMap>,
    override val usePartitionId: Int = 0,
    tableName: String? = null
) : EntitySchemaMap {
    override val tableName = tableName ?: getGeneratedTableName()

    override var entityPathSchema: EntityPathSchema
        get() = _entityPathSchema ?: throw IllegalStateException("Entity path schema is not resolved")
        internal set(value) {
            _entityPathSchema = value
        }
    private var _entityPathSchema: EntityPathSchema? = null

    private fun getGeneratedTableName(): String {
        val pathEntityNames = pathKeyMaps.map { it.name }
        if (pathEntityNames.isEmpty()) return ""
        val lastEntityName = pathEntityNames.last()
        val initialEntityNames = pathEntityNames.dropLast(1)
        // The table prefix is the initial letters of all the initial entity names.
        // If an entity name has underscores, then it is treated as multiple words,
        // and the initials of each word is used.
        val tablePrefix = initialEntityNames.flatMap { it.split("_") }.map { it[0] }.joinToString("")
        return "${tablePrefix}__$lastEntityName"
    }
}

class MutableEntityKeysSchemaMap(
    override val name: String,
    override val keyFieldMaps: List<MutableKeyFieldSchemaMap>
) : EntityKeysSchemaMap

class MutableKeyFieldSchemaMap(override val name: String, override val type: KeyType) : KeyFieldSchemaMap
