package org.tree_ware.cassandra.schema.map

import org.apache.logging.log4j.LogManager
import org.tree_ware.schema.core.MutableEntityPathSchema
import org.tree_ware.schema.core.RootSchema
import org.tree_ware.schema.core.validate

val KEYSPACE_NAME_LENGTH = 48
val TABLE_NAME_LENGTH = 48

/** Validates the specified schema-map.
 * Returns a list of errors. Returns an empty list if there are no errors.
 *
 * Side-effects: none
 */
fun validate(schemaMap: SchemaMap): List<String> {
    val logger = LogManager.getLogger()

    val pathErrors = validatePaths(schemaMap)
    pathErrors.forEach { logger.error(it) }
    if (pathErrors.isNotEmpty()) return pathErrors

    val nameErrors = validateNames(schemaMap)
    nameErrors.forEach { logger.error(it) }
    return nameErrors
}

fun validatePaths(schemaMap: SchemaMap): List<String> = schemaMap.entityMaps.mapIndexed { index, entitySchemaMap ->
    validatePath(schemaMap.rootMap.rootSchema, entitySchemaMap.pathKeyMaps, index)
}.flatten()

private fun validatePath(root: RootSchema, pathKeyMaps: List<EntityKeysSchemaMap>, index: Int): List<String> {
    val entityPath = pathKeyMaps.map { it.name }
    val entityPathSchema = MutableEntityPathSchema(entityPath)
    return validate(entityPathSchema, root, "entity-maps[$index]")
}

fun validateNames(schemaMap: SchemaMap): List<String> {
    val errors = mutableListOf<String>()
    if (schemaMap.rootMap.keyspaceName.length > KEYSPACE_NAME_LENGTH) errors.add(
        "Keyspace name ${schemaMap.rootMap.keyspaceName} exceeds $KEYSPACE_NAME_LENGTH character limit"
    )
    schemaMap.entityMaps.forEach {
        if (it.tableName.length > TABLE_NAME_LENGTH) errors.add(
            "Table name ${it.tableName} exceeds $TABLE_NAME_LENGTH character limit"
        )
    }
    return errors
}
