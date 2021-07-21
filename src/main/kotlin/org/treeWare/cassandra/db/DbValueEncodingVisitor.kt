package org.treeWare.cassandra.db

import org.treeWare.model.core.*
import org.treeWare.model.visitor.AbstractModelVisitor
import org.treeWare.schema.core.*

// Wrap returned values in QueryBuilder.raw()

class DbValueEncodingVisitor : AbstractModelVisitor<Unit, String>("") {
    override fun visit(field: PrimitiveFieldModel<Unit>): String = getRawValue(field.schema.primitive, field.value)

    override fun visit(field: AliasFieldModel<Unit>): String =
        getRawValue(field.schema.resolvedAlias.primitive, field.value)

    override fun visit(field: EnumerationFieldModel<Unit>): String = "'${field.value?.name}'"

    override fun visit(field: AssociationFieldModel<Unit>): String =
        field.schema.keyPath.zip(field.value) { keyEntityName, entityKeys ->
            entityKeys.fields.flatMap {
                getKeyValueList(keyEntityName, it, this) { key, value -> "$key:$value" }
            }
        }.flatten().joinToString(",", "{", "}")
}


internal fun <T> getKeyValueList(
    entityName: String,
    keyField: FieldModel<Unit>,
    valueEncodingVisitor: DbValueEncodingVisitor,
    transform: (key: String, value: String) -> T
): List<T> =
    if (keyField is CompositionFieldModel) {
        keyField.value.fields.filter { it.schema.isKey }.map { nestedKey ->
            val keyName = "\"/$entityName/${keyField.schema.name}/${nestedKey.schema.name}\""
            val keyValue = nestedKey.dispatch(valueEncodingVisitor)
            transform(keyName, keyValue)
        }
    } else {
        val keyName = "\"/$entityName/${keyField.schema.name}\""
        val keyValue = keyField.dispatch(valueEncodingVisitor)
        listOf(transform(keyName, keyValue))
    }

private fun getRawValue(schema: PrimitiveSchema, value: Any?): String = when (schema) {
    is BooleanSchema -> value?.toString() ?: "false"
    is ShortSchema,
    is IntSchema,
    is LongSchema,
    is FloatSchema,
    is DoubleSchema,
    is TimestampSchema -> value?.toString() ?: "0"
    is StringSchema -> "'${value?.toString() ?: ""}'"
    is UuidSchema -> value?.toString() ?: ""
    is ByteSchema,
    is Password1WaySchema,
    is Password2WaySchema,
    is BlobSchema -> "TODO"
    else -> ""
}
