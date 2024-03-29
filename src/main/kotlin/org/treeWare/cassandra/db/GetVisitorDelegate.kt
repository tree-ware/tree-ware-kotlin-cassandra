package org.treeWare.cassandra.db

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom
import kotlinx.coroutines.future.await
import org.treeWare.cassandra.schema.map.DbSchemaMapAux
import org.treeWare.model.action.CompositionTableGetVisitorDelegate
import org.treeWare.model.core.*
import org.treeWare.schema.core.*

class GetVisitorDelegate(private val cqlSession: CqlSession) : CompositionTableGetVisitorDelegate<DbSchemaMapAux> {
    override fun pushPathEntity(entity: BaseEntityModel<*>, entitySchema: EntitySchema) {
    }

    override fun popPathEntity() {
    }

    override suspend fun fetchRoot(
        responseRoot: MutableRootModel<Unit>,
        requestFieldNames: List<String>,
        mappingAux: DbSchemaMapAux
    ) {
        val quotedFieldNames = requestFieldNames.map { doubleQuote(it) }
        val select = selectFrom(mappingAux.keyspace, mappingAux.table)
            .columns(quotedFieldNames)
            .whereColumn(SYNTHETIC_PART_ID_NAME).isEqualTo(literal(0))
            .build()
        val result = cqlSession.executeAsync(select).await()
        result.currentPage().firstOrNull()?.also { row -> populateEntityFromRow(requestFieldNames, responseRoot, row) }
    }

    override suspend fun fetchCompositionList(
        responseListField: MutableListFieldModel<Unit>,
        requestFieldNames: List<String>,
        mappingAux: DbSchemaMapAux
    ) {
    }

    private fun populateEntityFromRow(fieldNames: List<String>, entity: MutableBaseEntityModel<Unit>, row: Row) {
        fieldNames.forEach { fieldName ->
            val columnName = doubleQuote(fieldName)
            val fieldPath = fieldName.split("/")
            populateScalarFieldFromRow(fieldPath, entity, columnName, row)
        }
    }

    private fun populateScalarFieldFromRow(
        fieldPath: List<String>,
        entity: MutableBaseEntityModel<Unit>,
        column: String,
        row: Row
    ) {
        if (fieldPath.isEmpty()) return
        val firstName = fieldPath.first()
        if (fieldPath.size > 1) {
            val childField = entity.getOrNewField(firstName) as? MutableSingleFieldModel?
            val childEntity = childField?.value as? MutableEntityModel ?: return
            return populateScalarFieldFromRow(fieldPath.drop(1), childEntity, column, row)
        }
        val field = entity.getOrNewField(firstName) as? MutableSingleFieldModel ?: return
        val value = field.getOrNewValue() as? MutableScalarValueModel ?: return
        val primitiveSchema = getPrimitiveSchema(field) ?: return
        when (primitiveSchema) {
            is BooleanSchema -> value.setValue(row.getBoolean(column))
            is ByteSchema,
            is ShortSchema,
            is IntSchema,
            is LongSchema,
            is FloatSchema,
            is DoubleSchema -> row.getBigDecimal(column)?.also { value.setValue(it) }
            is StringSchema,
            is Password1WaySchema,
            is Password2WaySchema -> row.getString(column)?.also { value.setValue(it) }
            is UuidSchema -> row.getUuid(column)?.also { value.setValue(it.toString()) }
            is BlobSchema -> row.getByteBuffer(column)?.also { value.setValue(it.toString()) }
            is TimestampSchema -> row.getInstant(column)?.also { value.setValue(it.toEpochMilli().toString()) }
            else -> row.getString(column)?.also { value.setValue(it) }
        }
    }

    private fun getPrimitiveSchema(field: MutableSingleFieldModel<Unit>): PrimitiveSchema? =
        when (val fieldSchema = field.schema) {
            is PrimitiveFieldSchema -> fieldSchema.primitive
            is AliasFieldSchema -> fieldSchema.resolvedAlias.primitive
            is EnumerationFieldSchema -> MutableStringSchema()
            else -> null
        }

    private fun doubleQuote(string: String): String = "\"$string\""
}
