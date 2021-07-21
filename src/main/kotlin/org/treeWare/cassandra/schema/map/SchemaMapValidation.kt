package org.treeWare.cassandra.schema.map

import org.apache.logging.log4j.LogManager
import org.treeWare.schema.core.*

const val KEYSPACE_NAME_LENGTH = 48
const val TABLE_NAME_LENGTH = 48

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

data class KeyInfo(val schema: FieldSchema, val nestedKeyParentName: String?) {
    val keyName get() = if (nestedKeyParentName == null) schema.name else "$nestedKeyParentName/${schema.name}"
}

fun validatePathEntitySchemaMap(
    pathEntity: MutablePathEntitySchemaMap,
    entitySchema: EntitySchema,
    index: Int
): List<String> {
    val keyInfoList = mutableListOf<KeyInfo>()
    entitySchema.fields.forEach { field ->
        if (field.isKey) {
            if (field !is CompositionFieldSchema) keyInfoList.add(KeyInfo(field, null))
            else keyInfoList.addAll(field.resolvedEntity.fields.filter { it.isKey }.map { KeyInfo(it, field.name) })
        }
    }
    if (pathEntity.keys.size != keyInfoList.size) return listOf(
        "Invalid number of keys in pathEntity ${pathEntity.name} map: entityPaths[$index]"
    )
    val fieldErrors = pathEntity.keys.zip(keyInfoList) { keyField, keyInfo ->
        validateKeyFieldSchemaMap(keyField, keyInfo, index)
    }.flatten()
    if (fieldErrors.isEmpty()) pathEntity.schema = entitySchema
    return fieldErrors
}

fun validateKeyFieldSchemaMap(
    keyField: MutableKeyFieldSchemaMap,
    keyInfo: KeyInfo,
    index: Int
): List<String> {
    return if (keyField.name == keyInfo.keyName) {
        keyField.nestedKeyParentName = keyInfo.nestedKeyParentName
        keyField.schema = keyInfo.schema
        listOf()
    } else listOf(
        "Map key ${keyField.name} does not match schema key ${keyInfo.keyName}: entityPaths[$index]"
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
