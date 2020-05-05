package org.tree_ware.cassandra.db

import com.datastax.oss.driver.api.core.type.DataTypes
import com.datastax.oss.driver.api.querybuilder.BuildableQuery
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable
import org.tree_ware.cassandra.schema.map.EntitySchemaMap
import org.tree_ware.cassandra.schema.map.KeyType
import org.tree_ware.cassandra.schema.map.RootSchemaMap
import org.tree_ware.cassandra.schema.map.SchemaMap

const val SYNTHETIC_PARTITION_ID_NAME = "partition_id_"

fun encodeDbSchema(schemaMap: SchemaMap): List<BuildableQuery> {
    val dbCommands = mutableListOf<BuildableQuery>()
    dbCommands.add(encodeDbKeyspace(schemaMap.rootMap))
    schemaMap.entityMaps.forEach { dbCommands.add(encodeDbTable(schemaMap.rootMap, it)) }
    return dbCommands
}

private fun encodeDbKeyspace(rootSchemaMap: RootSchemaMap): BuildableQuery =
    SchemaBuilder.createKeyspace(rootSchemaMap.keyspaceName)
        .ifNotExists()
        .withSimpleStrategy(2)
        .withDurableWrites(true)

private fun encodeDbTable(rootSchemaMap: RootSchemaMap, entitySchemaMap: EntitySchemaMap): BuildableQuery {
    var table: CreateTable = SchemaBuilder
        .createTable(rootSchemaMap.keyspaceName, """"${entitySchemaMap.tableName}"""")
        .ifNotExists()
        .withPartitionKey(SYNTHETIC_PARTITION_ID_NAME, DataTypes.INT)

    val columnGenerator = DbColumnSchemaGeneratingVisitor()

    // Generate key columns.
    entitySchemaMap.pathKeyMaps.forEach { pathKeyMap ->
        pathKeyMap.keyFieldMaps.forEach { keyFieldMap ->
            columnGenerator.reinitialize(listOf("", pathKeyMap.name), keyFieldMap.type)
            keyFieldMap.fieldSchema.accept(columnGenerator)
        }
    }
    // Generate non-key columns
    columnGenerator.reinitialize(null, null)
    entitySchemaMap.entityPathSchema.resolvedEntity.fields.filterNot { it.isKey }.forEach {
        it.accept(columnGenerator)
    }

    // Add all columns to the table
    columnGenerator.columns.forEach {
        table = when (it.keyType) {
            KeyType.PARTITION -> table.withPartitionKey(it.name, it.dataType)
            KeyType.CLUSTERING -> table.withClusteringColumn(it.name, it.dataType)
            else -> table.withColumn(it.name, it.dataType)
        }
    }

    return table
}
