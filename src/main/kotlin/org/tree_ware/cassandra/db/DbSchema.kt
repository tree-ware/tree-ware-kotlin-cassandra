package org.tree_ware.cassandra.db

import com.datastax.oss.driver.api.core.type.DataTypes
import com.datastax.oss.driver.api.querybuilder.BuildableQuery
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable
import org.tree_ware.cassandra.schema.map.EntityPathSchemaMap
import org.tree_ware.cassandra.schema.map.KeyType
import org.tree_ware.cassandra.schema.map.RootSchemaMap
import org.tree_ware.cassandra.schema.map.SchemaMap

const val SYNTHETIC_PART_ID_NAME = "part_id_"

fun encodeDbSchema(schemaMap: SchemaMap): List<BuildableQuery> {
    val dbCommands = mutableListOf<BuildableQuery>()
    dbCommands.add(encodeDbKeyspace(schemaMap.root))
    schemaMap.entityPaths.forEach { dbCommands.add(encodeDbTable(schemaMap.root, it)) }
    return dbCommands
}

private fun encodeDbKeyspace(root: RootSchemaMap): BuildableQuery =
    SchemaBuilder.createKeyspace(root.keyspaceName)
        .ifNotExists()
        .withSimpleStrategy(2)
        .withDurableWrites(true)

private fun encodeDbTable(root: RootSchemaMap, entityPath: EntityPathSchemaMap): BuildableQuery {
    var table: CreateTable = SchemaBuilder
        .createTable(root.keyspaceName, """"${entityPath.tableName}"""")
        .ifNotExists()
        .withPartitionKey(SYNTHETIC_PART_ID_NAME, DataTypes.INT)

    val columnGenerator = DbColumnSchemaGeneratingVisitor()

    // Generate key columns.
    entityPath.pathEntities.forEach { pathEntity ->
        pathEntity.keys.forEach { key ->
            columnGenerator.reinitialize(listOf("", pathEntity.name), key.type)
            key.schema.accept(columnGenerator)
        }
    }
    // Generate non-key columns
    columnGenerator.reinitialize(null, null)
    entityPath.schema.resolvedEntity.fields.filterNot { it.isKey }.forEach { it.accept(columnGenerator) }

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
