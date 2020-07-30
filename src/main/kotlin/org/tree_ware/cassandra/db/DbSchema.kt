package org.tree_ware.cassandra.db

import com.datastax.oss.driver.api.core.type.DataTypes
import com.datastax.oss.driver.api.querybuilder.BuildableQuery
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable
import com.datastax.oss.driver.api.querybuilder.schema.CreateType
import com.datastax.oss.driver.api.querybuilder.schema.OngoingCreateType
import org.tree_ware.cassandra.schema.map.EntityPathSchemaMap
import org.tree_ware.cassandra.schema.map.KeyType
import org.tree_ware.cassandra.schema.map.SchemaMap
import org.tree_ware.cassandra.schema.map.getKeyspaceName
import org.tree_ware.schema.core.EntityPathSchema
import java.util.*

const val SYNTHETIC_PART_ID_NAME = "part_id_"

fun encodeCreateDbSchema(environment: String, schemaMap: SchemaMap): List<BuildableQuery> {
    val keyspaceName = getKeyspaceName(environment, schemaMap.root)
    val createKeyspace = encodeCreateDbKeyspace(keyspaceName)
    val createTypes = sortedMapOf<String, CreateType>()
    val createTables = schemaMap.entityPaths.map { encodeCreateDbTable(keyspaceName, it, createTypes) }
    return listOf(createKeyspace) + createTypes.values + createTables
}

private fun encodeCreateDbKeyspace(keyspaceName: String): CreateKeyspace =
    SchemaBuilder.createKeyspace(keyspaceName)
        .ifNotExists()
        .withSimpleStrategy(2)
        .withDurableWrites(true)

private fun encodeCreateDbTable(
    keyspaceName: String,
    entityPath: EntityPathSchemaMap,
    createTypes: SortedMap<String, CreateType>
): CreateTable {
    var table: CreateTable = SchemaBuilder
        .createTable(keyspaceName, """"${entityPath.tableName}"""")
        .ifNotExists()
        .withPartitionKey(SYNTHETIC_PART_ID_NAME, DataTypes.INT)

    val columnGenerator = DbColumnSchemaGeneratingVisitor(keyspaceName, createTypes)

    // Generate key columns.
    entityPath.pathEntities.forEach { pathEntity ->
        pathEntity.keys.forEach { key ->
            val nestedKeyParentName = key.nestedKeyParentName
            val initialNames =
                if (nestedKeyParentName == null) listOf("", pathEntity.name)
                else listOf("", pathEntity.name, nestedKeyParentName)
            columnGenerator.reinitialize(initialNames, key.type)
            key.schema.traverse(columnGenerator)
        }
    }
    // Generate non-key columns.
    columnGenerator.reinitialize(null, null)
    entityPath.schema.resolvedEntity.fields.filterNot { it.isKey }.forEach { it.traverse(columnGenerator) }

    // Add all columns to the table.
    columnGenerator.columns.forEach {
        table = when (it.keyType) {
            KeyType.PARTITION -> table.withPartitionKey(it.name, it.dataType)
            KeyType.CLUSTERING -> table.withClusteringColumn(it.name, it.dataType)
            else -> table.withColumn(it.name, it.dataType)
        }
    }

    return table
}

internal fun getDbAssociationTypeName(entityPathSchema: EntityPathSchema): String =
    entityPathSchema.entityPath.joinToString("/", "\"/", "\"")

internal fun encodeCreateDbAssociationType(
    keyspaceName: String,
    entityPathSchema: EntityPathSchema,
    createTypes: SortedMap<String, CreateType>
): CreateType {
    val typeName = getDbAssociationTypeName(entityPathSchema)
    var type: OngoingCreateType = SchemaBuilder
        .createType(keyspaceName, typeName)
        .ifNotExists()

    // Generate key columns for use as fields in the type.
    val columnGenerator = DbColumnSchemaGeneratingVisitor(keyspaceName, createTypes)
    entityPathSchema.keyPath.zip(entityPathSchema.keyEntities) { keyEntityName, keyEntity ->
        val keyFields = keyEntity.fields.filter { it.isKey }
        assert(keyFields.isNotEmpty())
        columnGenerator.reinitialize(listOf("", keyEntityName), null)
        keyFields.forEach { it.traverse(columnGenerator) }
    }

    // Add columns as fields to the type.
    columnGenerator.columns.forEach {
        type = type.withField(it.name, it.dataType)
    }

    return type as CreateType
}
