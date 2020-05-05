package org.tree_ware.cassandra.schema.map

import org.apache.logging.log4j.LogManager
import org.tree_ware.schema.core.*

val KEYSPACE_NAME_LENGTH = 48
val TABLE_NAME_LENGTH = 48

/** Validates the specified schema-map.
 * Returns a list of errors. Returns an empty list if there are no errors.
 *
 * Side-effects:
 * 1. Resolve schema in map elements
 */
fun validate(schemaMap: MutableSchemaMap): List<String> {
    val logger = LogManager.getLogger()

    val pathErrors = schemaMap.entityPaths.mapIndexed { index, entityPath ->
        validateEntityPathSchemaMap(entityPath, schemaMap.root.schema, index)
    }.flatten()
    pathErrors.forEach { logger.error(it) }
    if (pathErrors.isNotEmpty()) return pathErrors

    val nameErrors = validateNames(schemaMap)
    nameErrors.forEach { logger.error(it) }
    return nameErrors
}

fun validateEntityPathSchemaMap(
    entityPath: MutableEntityPathSchemaMap,
    rootSchema: RootSchema,
    index: Int
): List<String> {
    val pathEntityNames = entityPath.pathEntities.map { it.name }

    // Tree-ware can, but doesn't yet support the case where an entity name repeats in the path.
    // All keys of a table need to have unique names. Entity key-names are not unique across entities
    // and hence are prefixed by the entity names to make them unique. But if the entity names in the
    // path are not unique, our naming scheme for table keys can still result in conflicts.
    // The solution is to include a repetition-count in the name to make the names unique.
    // The repetition-count is not yet implemented.
    if (pathEntityNames.size != pathEntityNames.toSet().size) return listOf(
        "Duplicate entity names in path are not yet supported in tree-ware Cassandra: entityPaths[$index]"
    )

    val entityPathSchema = MutableEntityPathSchema(pathEntityNames)
    val pathErrors = validate(entityPathSchema, rootSchema, "entityPaths[$index]")
    if (pathErrors.isNotEmpty()) return pathErrors
    entityPath.schema = entityPathSchema

    // Validate the entities in the path
    return entityPath.pathEntities.zip(entityPathSchema.pathEntities) { pathEntity, entitySchema ->
        validatePathEntitySchemaMap(pathEntity, entitySchema, index)
    }.flatten()
}

fun validatePathEntitySchemaMap(
    pathEntity: MutablePathEntitySchemaMap,
    entitySchema: EntitySchema,
    index: Int
): List<String> {
    val keyFieldSchemas = entitySchema.fields.filter { it.isKey }
    if (pathEntity.keys.size != keyFieldSchemas.size) return listOf(
        "Invalid number of keys in pathEntity ${pathEntity.name} map: entityPaths[$index]"
    )
    val fieldErrors = pathEntity.keys.zip(keyFieldSchemas) { keyField, fieldSchema ->
        validateKeyFieldSchemaMap(keyField, fieldSchema, index)
    }.flatten()
    if (fieldErrors.isEmpty()) pathEntity.schema = entitySchema
    return fieldErrors
}

fun validateKeyFieldSchemaMap(
    keyField: MutableKeyFieldSchemaMap,
    fieldSchema: FieldSchema,
    index: Int
): List<String> {
    return if (keyField.name == fieldSchema.name) {
        keyField.schema = fieldSchema
        listOf()
    } else listOf(
        "Map key ${keyField.name} does not match schema key ${fieldSchema.name}: entityPaths[$index]"
    )
}

fun validateNames(schemaMap: SchemaMap): List<String> {
    val errors = mutableListOf<String>()
    if (schemaMap.root.keyspaceName.length > KEYSPACE_NAME_LENGTH) errors.add(
        "Keyspace name ${schemaMap.root.keyspaceName} exceeds $KEYSPACE_NAME_LENGTH character limit"
    )
    schemaMap.entityPaths.forEach {
        if (it.tableName.length > TABLE_NAME_LENGTH) errors.add(
            "Table name ${it.tableName} exceeds $TABLE_NAME_LENGTH character limit"
        )
    }
    return errors
}
