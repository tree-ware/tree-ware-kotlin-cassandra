package org.treeWare.cassandra.db

import org.treeWare.model.action.isCompositionField
import org.treeWare.model.core.*
import org.treeWare.model.operator.AbstractLeader1Follower0ModelVisitor
import org.treeWare.model.operator.dispatchVisit
import org.treeWare.schema.core.*

// Wrap returned values in QueryBuilder.raw()

class DbValueEncodingVisitor : AbstractLeader1Follower0ModelVisitor<Unit, String>("") {
    override fun visit(leaderValue1: PrimitiveModel<Unit>): String =
        getRawValue(leaderValue1.schema.primitive, leaderValue1.value)

    override fun visit(leaderValue1: AliasModel<Unit>): String =
        getRawValue(leaderValue1.schema.resolvedAlias.primitive, leaderValue1.value)

    override fun visit(leaderValue1: EnumerationModel<Unit>): String = "'${leaderValue1.value?.name}'"

    override fun visit(leaderValue1: AssociationModel<Unit>): String =
        leaderValue1.schema.keyPath.zip(leaderValue1.value) { keyEntityName, entityKeys ->
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
    if (isCompositionField(keyField)) {
        val entity = (keyField as SingleFieldModel).value as EntityModel
        entity.fields.filter { it.schema.isKey }.map { nestedKey ->
            val keyName = "\"/$entityName/${keyField.schema.name}/${nestedKey.schema.name}\""
            val nestedKeyValue = (nestedKey as? SingleFieldModel<Unit>)?.value
            val keyValue = nestedKeyValue?.let { dispatchVisit(it, valueEncodingVisitor) } ?: ""
            transform(keyName, keyValue)
        }
    } else {
        val keyName = "\"/$entityName/${keyField.schema.name}\""
        val keyFieldValue = (keyField as? SingleFieldModel<Unit>)?.value
        val keyValue = keyFieldValue?.let { dispatchVisit(it, valueEncodingVisitor) } ?: ""
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
