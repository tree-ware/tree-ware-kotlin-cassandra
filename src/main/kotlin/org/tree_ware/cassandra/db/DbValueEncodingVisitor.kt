package org.tree_ware.cassandra.db

import org.tree_ware.model.core.AliasFieldModel
import org.tree_ware.model.core.AssociationFieldModel
import org.tree_ware.model.core.EnumerationFieldModel
import org.tree_ware.model.core.PrimitiveFieldModel
import org.tree_ware.model.visitor.AbstractModelVisitor
import org.tree_ware.schema.core.*

// Wrap returned values in QueryBuilder.raw()

class DbValueEncodingVisitor : AbstractModelVisitor<Unit, String>("") {
    override fun visit(field: PrimitiveFieldModel<Unit>): String = getRawValue(field.schema.primitive, field.value)

    override fun visit(field: AliasFieldModel<Unit>): String =
        getRawValue(field.schema.resolvedAlias.primitive, field.value)

    override fun visit(field: EnumerationFieldModel<Unit>): String = "'${field.value?.name}'"

    override fun visit(field: AssociationFieldModel<Unit>): String =
        field.schema.keyPath.zip(field.value) { keyEntityName, entityKeys ->
            entityKeys.fields.map { "\"/$keyEntityName/${it.schema.name}\":${it.dispatch(this)}" }
        }.flatten().joinToString(",", "{", "}")
}

private fun getRawValue(schema: PrimitiveSchema, value: Any?): String = when (schema) {
    is BooleanSchema -> value?.toString() ?: "false"
    is ShortSchema,
    is IntSchema,
    is LongSchema,
    is FloatSchema,
    is DoubleSchema,
    is TimestampSchema -> value?.toString() ?: "0"
    is UuidSchema,
    is StringSchema -> "'${value?.toString() ?: ""}'"
    is ByteSchema,
    is Password1WaySchema,
    is Password2WaySchema,
    is BlobSchema -> "TODO"
    else -> ""
}
