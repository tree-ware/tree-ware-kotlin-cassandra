package org.tree_ware.cassandra.schema.map

import org.tree_ware.model.codec.decoding_state_machine.ModelDecodingStateMachine
import org.tree_ware.model.core.MutableModel

const val DB_SCHEMA_MAP_MODEL_TYPE = "db_schema_map"
const val VALUE_KEY = "value"

fun asModel(environment: String, schemaMap: SchemaMap): MutableModel<DbSchemaMapAux> {
    // MutableModel setters are internal methods. Rather than make them public,
    // create the model by acting like a decoder. Eventually the mapping will
    // be captured as a list of models, and this code won't be needed.
    val schema = schemaMap.schema
    val decoder = ModelDecodingStateMachine(schema, DB_SCHEMA_MAP_MODEL_TYPE) { DbSchemaMapAuxStateMachine(it) }
    val keyspace = getKeyspaceName(environment, schemaMap.root)
    schemaMap.entityPaths.forEach { addPath(decoder, keyspace, it) }
    return decoder.model as MutableModel<DbSchemaMapAux>
}

private fun addPath(
    decoder: ModelDecodingStateMachine<DbSchemaMapAux>,
    keyspace: String,
    entityPath: EntityPathSchemaMap
) {
    // Start from the root each time
    decoder.reinitialize()

    // Create the model instance
    decoder.decodeObjectStart()
    decoder.decodeKey(DB_SCHEMA_MAP_MODEL_TYPE)
    decoder.decodeObjectStart()

    // Create the model corresponding to the specified path
    addEntity(decoder, keyspace, entityPath, 0, entityPath.pathEntities.lastIndex)

    decoder.decodeObjectEnd()
    decoder.decodeObjectEnd()
}

private fun addEntity(
    decoder: ModelDecodingStateMachine<DbSchemaMapAux>,
    keyspace: String,
    entityPath: EntityPathSchemaMap,
    index: Int,
    lastIndex: Int
) {
    val pathEntity = entityPath.pathEntities[index]
    decoder.decodeKey(pathEntity.name)
    decoder.decodeObjectStart()
    if (index == lastIndex) addAux(decoder, keyspace, entityPath.tableName)

    // Only the root entity in the path is not a list. So its value alone is
    // wrapped in a VALUE_KEY object. All other entities in the path are lists
    // and so their values are wrapped in a VALUE_KEY list.
    if (index == 0) {
        decoder.decodeKey(VALUE_KEY)
        decoder.decodeObjectStart()
    } else {
        decoder.decodeKey(VALUE_KEY)
        decoder.decodeListStart()
        if (index != lastIndex) {
            decoder.decodeObjectStart()
            decoder.decodeKey(VALUE_KEY)
            decoder.decodeObjectStart()
        }
    }

    if (index < lastIndex) addEntity(decoder, keyspace, entityPath, index + 1, lastIndex)

    if (index == 0) {
        decoder.decodeObjectEnd()
    } else {
        if (index != lastIndex) {
            decoder.decodeObjectEnd()
            decoder.decodeObjectEnd()
        }
        decoder.decodeListEnd()
    }

    decoder.decodeObjectEnd()
}

private fun addAux(decoder: ModelDecodingStateMachine<DbSchemaMapAux>, keyspace: String, table: String) {
    decoder.decodeKey(DB_SCHEMA_MAP_MODEL_TYPE)
    decoder.decodeObjectStart()
    decoder.decodeKey("keyspace")
    decoder.decodeStringValue(keyspace)
    decoder.decodeKey("table")
    decoder.decodeStringValue(table)
    decoder.decodeObjectEnd()
}
