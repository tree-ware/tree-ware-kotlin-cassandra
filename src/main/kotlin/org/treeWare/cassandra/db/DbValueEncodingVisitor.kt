package org.treeWare.cassandra.db

import org.treeWare.model.core.*
import org.treeWare.model.operator.AbstractLeader1Follower0ModelVisitor
import org.treeWare.model.operator.dispatchVisit
import org.treeWare.schema.core.*

// Wrap returned values in QueryBuilder.raw()

class DbValueEncodingVisitor : AbstractLeader1Follower0ModelVisitor<Unit, String>("") {
    override fun visit(leaderField1: PrimitiveFieldModel<Unit>): String =
        getRawValue(leaderField1.schema.primitive, leaderField1.value)

    override fun visit(leaderField1: AliasFieldModel<Unit>): String =
        getRawValue(leaderField1.schema.resolvedAlias.primitive, leaderField1.value)

    override fun visit(leaderField1: EnumerationFieldModel<Unit>): String = "'${leaderField1.value?.name}'"

    override fun visit(leaderField1: AssociationFieldModel<Unit>): String =
        leaderField1.schema.keyPath.zip(leaderField1.value) { keyEntityName, entityKeys ->
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
            val keyValue = dispatchVisit(nestedKey, valueEncodingVisitor) ?: ""
            transform(keyName, keyValue)
        }
    } else {
        val keyName = "\"/$entityName/${keyField.schema.name}\""
        val keyValue = dispatchVisit(keyField, valueEncodingVisitor) ?: ""
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
