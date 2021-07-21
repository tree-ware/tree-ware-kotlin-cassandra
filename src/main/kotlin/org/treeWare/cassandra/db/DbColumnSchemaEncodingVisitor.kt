package org.treeWare.cassandra.db

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.type.DataType
import com.datastax.oss.driver.api.core.type.DataTypes
import com.datastax.oss.driver.api.querybuilder.schema.CreateType
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder
import org.treeWare.cassandra.schema.map.KeyType
import org.treeWare.common.traversal.TraversalAction
import org.treeWare.schema.core.*
import org.treeWare.schema.visitor.AbstractSchemaVisitor
import java.util.*

data class ColumnSchema(val name: CqlIdentifier, val keyType: KeyType?, val dataType: DataType)

class DbColumnSchemaEncodingVisitor(
    private val keyspaceName: String,
    private val createTypes: SortedMap<String, CreateType>
) : AbstractSchemaVisitor<TraversalAction>(TraversalAction.CONTINUE) {
    val columns: List<ColumnSchema> get() = _columns
    private val _columns = mutableListOf<ColumnSchema>()

    private var keyType: KeyType? = null

    private val nameParts = mutableListOf<String>()
    private val cqlName get() = CqlIdentifier.fromCql("\"${nameParts.joinToString("/")}\"")

    fun reinitialize(initialNames: List<String>?, keyType: KeyType?) {
        this.keyType = keyType
        nameParts.clear()
        if (initialNames != null) nameParts.addAll(initialNames)
    }

    private fun addColumn(dataType: DataType, isSet: Boolean = false) {
        val columnType = if (isSet) DataTypes.setOf(dataType) else dataType
        _columns.add(ColumnSchema(cqlName, keyType, columnType))
    }

    // SchemaVisitor methods

    // Fields

    override fun visit(field: FieldSchema): TraversalAction {
        nameParts.add(field.name)
        return TraversalAction.CONTINUE
    }

    override fun leave(field: FieldSchema) {
        nameParts.removeAt(nameParts.lastIndex)
    }

    override fun visit(primitiveField: PrimitiveFieldSchema): TraversalAction {
        addColumn(treeWareToCqlDataType(primitiveField.primitive), primitiveField.multiplicity.isList())
        return TraversalAction.CONTINUE
    }

    override fun visit(aliasField: AliasFieldSchema): TraversalAction {
        addColumn(treeWareToCqlDataType(aliasField.resolvedAlias.primitive), aliasField.multiplicity.isList())
        return TraversalAction.CONTINUE
    }

    override fun visit(enumerationField: EnumerationFieldSchema): TraversalAction {
        addColumn(DataTypes.TEXT, enumerationField.multiplicity.isList())
        return TraversalAction.CONTINUE
    }

    override fun visit(associationField: AssociationFieldSchema): TraversalAction {
        val typeName = getDbAssociationTypeName(associationField)
        if (!createTypes.containsKey(typeName)) {
            createTypes[typeName] = encodeCreateDbAssociationType(keyspaceName, associationField, createTypes)
        }
        val dataType = UserDefinedTypeBuilder(keyspaceName, typeName)
            .frozen()
            // User-defined-type DataType instances cannot be built without a field, but we only need keyspace &
            // typename, so we use a dummy field to suppress runtime errors.
            .withField("dummy", DataTypes.INT)
            .build()
        addColumn(dataType, associationField.multiplicity.isList())
        return TraversalAction.CONTINUE
    }

    override fun visit(compositionField: CompositionFieldSchema): TraversalAction {
        if (compositionField.multiplicity.isList()) return TraversalAction.ABORT_SUB_TREE
        // TODO(deepak-nulu): visitor-flag to traverse or not traverse the resolvedEntity
        return compositionField.resolvedEntity.traverse(this)
    }
}
