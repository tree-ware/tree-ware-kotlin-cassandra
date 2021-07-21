package org.treeWare.cassandra.schema.map

import org.treeWare.schema.core.*

class MutableSchemaMap(
    override val schema: Schema,
    keyspaceName: String? = null,
    override val entityPaths: List<MutableEntityPathSchemaMap>
) : SchemaMap {
    override val root = MutableRootSchemaMap(schema.root, keyspaceName)
}

class MutableRootSchemaMap(
    override val schema: RootSchema,
    keyspaceName: String? = null
) : RootSchemaMap {
    override val keyspaceName = if (keyspaceName != null) "tw_$keyspaceName" else "tw_${schema.name}"
}

class MutableEntityPathSchemaMap(
    override val pathEntities: List<MutablePathEntitySchemaMap>,
    override val syntheticPartIdSize: Int = 1,
    tableName: String? = null
) : EntityPathSchemaMap {
    override val tableName = tableName ?: getGeneratedTableName()

    override var schema: EntityPathSchema
        get() = _schema ?: throw IllegalStateException("Entity path schema is not resolved")
        internal set(value) {
            _schema = value
        }
    private var _schema: EntityPathSchema? = null

    private fun getGeneratedTableName(): String {
        val pathEntityNames = pathEntities.map { it.name }
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

class MutablePathEntitySchemaMap(
    override val name: String,
    override val keys: List<MutableKeyFieldSchemaMap>
) : PathEntitySchemaMap {
    override var schema: EntitySchema
        get() = _schema ?: throw IllegalStateException("Entity schema is not resolved")
        internal set(value) {
            _schema = value
        }
    private var _schema: EntitySchema? = null
}

class MutableKeyFieldSchemaMap(override val name: String, override val type: KeyType) : KeyFieldSchemaMap {
    override var nestedKeyParentName: String? = null
        internal set

    override var schema: FieldSchema
        get() = _schema ?: throw IllegalStateException("Field schema is not set")
        internal set(value) {
            _schema = value
        }
    private var _schema: FieldSchema? = null
}
