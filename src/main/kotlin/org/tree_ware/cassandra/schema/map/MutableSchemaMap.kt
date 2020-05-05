package org.tree_ware.cassandra.schema.map

import org.tree_ware.schema.core.*

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
    override val keyspaceName = if (keyspaceName != null) "tw_$keyspaceName" else "tw_${rootSchema.name}"
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
        return if (tablePrefix == "") lastEntityName else "${tablePrefix}__$lastEntityName"
    }
}

class MutableEntityKeysSchemaMap(
    override val name: String,
    override val keyFieldMaps: List<MutableKeyFieldSchemaMap>
) : EntityKeysSchemaMap {
    override var entitySchema: EntitySchema
        get() = _entitySchema ?: throw IllegalStateException("Entity schema is not resolved")
        internal set(value) {
            _entitySchema = value
        }
    private var _entitySchema: EntitySchema? = null
}

class MutableKeyFieldSchemaMap(override val name: String, override val type: KeyType) : KeyFieldSchemaMap {
    override var fieldSchema: FieldSchema
        get() = _fieldSchema ?: throw IllegalStateException("Field schema is not set")
        internal set(value) {
            _fieldSchema = value
        }
    private var _fieldSchema: FieldSchema? = null
}
