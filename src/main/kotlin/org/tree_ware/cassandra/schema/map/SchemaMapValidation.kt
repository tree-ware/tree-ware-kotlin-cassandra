package org.tree_ware.cassandra.schema.map

import org.apache.logging.log4j.LogManager
import org.tree_ware.schema.core.*

val KEYSPACE_NAME_LENGTH = 48
val TABLE_NAME_LENGTH = 48

/** Validates the specified schema-map.
 * Returns a list of errors. Returns an empty list if there are no errors.
 *
 * Side-effects:
 * 1. `entityPathSchema` in `EntitySchemaMap` are resolved
 */
fun validate(schemaMap: MutableSchemaMap): List<String> {
    val logger = LogManager.getLogger()

    val pathErrors = validateEntitySchemaMaps(schemaMap)
    pathErrors.forEach { logger.error(it) }
    if (pathErrors.isNotEmpty()) return pathErrors

    val nameErrors = validateNames(schemaMap)
    nameErrors.forEach { logger.error(it) }
    return nameErrors
}

fun validateEntitySchemaMaps(schemaMap: MutableSchemaMap): List<String> =
    schemaMap.entityMaps.mapIndexed { index, entitySchemaMap ->
        validateEntitySchemaMap(entitySchemaMap, schemaMap.rootMap.rootSchema, index)
    }.flatten()

private fun validateEntitySchemaMap(
    entitySchemaMap: MutableEntitySchemaMap,
    root: RootSchema,
    index: Int
): List<String> {
    val entityPath = entitySchemaMap.pathKeyMaps.map { it.name }

    // Tree-ware can, but doesn't yet support the case where an entity name repeats in the path.
    // All keys of a table need to have unique names. Entity key-names are not unique across entities
    // and hence are prefixed by the entity names to make them unique. But if the entity names in the
    // path are not unique, our naming scheme for table keys can still result in conflicts.
    // The solution is to include a repetition-count in the name to make the names unique.
    // The repetition-count is not yet implemented.
    if (entityPath.size != entityPath.toSet().size) return listOf(
        "Duplicate entity names in path are not yet supported in tree-ware Cassandra: entity-maps[$index]"
    )

    val entityPathSchema = MutableEntityPathSchema(entityPath)
    val pathErrors = validate(entityPathSchema, root, "entity-maps[$index]")
    if (pathErrors.isNotEmpty()) return pathErrors
    entitySchemaMap.entityPathSchema = entityPathSchema

    // Ensure that keys specified in the path match the keys of the entities in the path
    return validatePathKeyMaps(entitySchemaMap.pathKeyMaps, entityPathSchema, index) +
            validatePartitionKeys(entitySchemaMap, index)
}

private fun validatePathKeyMaps(
    pathKeyMaps: List<EntityKeysSchemaMap>,
    entityPathSchema: EntityPathSchema,
    index: Int
): List<String> = pathKeyMaps.zip(entityPathSchema.pathEntities) { entityKeysSchemaMap, entitySchema ->
    validateEntityKeysSchemaMaps(entityKeysSchemaMap, entitySchema, index)
}.flatten()

fun validateEntityKeysSchemaMaps(
    entityKeysSchemaMap: EntityKeysSchemaMap,
    entitySchema: EntitySchema,
    index: Int
): List<String> {
    val mapKeyNames = entityKeysSchemaMap.keyFieldMaps.map { it.name }
    val entityKeyNames = entitySchema.fields.filter { it.isKey }.map { it.name }
    return if (mapKeyNames != entityKeyNames) listOf(
        "Keys in map $mapKeyNames don't match keys in entity $entityKeyNames: entity-maps[$index] path entity ${entityKeysSchemaMap.name}"
    ) else listOf()
}

fun validatePartitionKeys(entitySchemaMap: EntitySchemaMap, index: Int): List<String> {
    if (entitySchemaMap.usePartitionId > 0) return listOf()
    val hasPartitionKeys =
        entitySchemaMap.pathKeyMaps.flatMap { it.keyFieldMaps.map { it.type } }.any { it == KeyType.PARTITION }
    return if (hasPartitionKeys) listOf() else listOf("No partition keys: entity-maps[$index]")
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
