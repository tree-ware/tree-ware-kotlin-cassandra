package org.tree_ware.cassandra.db

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.type.DataType
import com.datastax.oss.driver.api.core.type.DataTypes
import com.datastax.oss.driver.api.querybuilder.schema.CreateType
import org.tree_ware.cassandra.schema.map.KeyType
import org.tree_ware.schema.core.*
import org.tree_ware.schema.visitor.AbstractSchemaVisitor

data class ColumnSchema(val name: CqlIdentifier, val keyType: KeyType?, val dataType: DataType)

class DbColumnSchemaGeneratingVisitor(
    private val keyspaceName: String,
    private val createTypes: HashMap<String, CreateType>
) : AbstractSchemaVisitor<SchemaTraversalAction>(SchemaTraversalAction.CONTINUE) {
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

    override fun visit(field: FieldSchema): SchemaTraversalAction {
        nameParts.add(field.name)
        return SchemaTraversalAction.CONTINUE
    }

    override fun leave(field: FieldSchema) {
        nameParts.removeAt(nameParts.lastIndex)
    }

    override fun visit(primitiveField: PrimitiveFieldSchema): SchemaTraversalAction {
        addColumn(treeWareToCqlDataType(primitiveField.primitive), primitiveField.multiplicity.isList())
        return SchemaTraversalAction.CONTINUE
    }

    override fun visit(aliasField: AliasFieldSchema): SchemaTraversalAction {
        addColumn(treeWareToCqlDataType(aliasField.resolvedAlias.primitive), aliasField.multiplicity.isList())
        return SchemaTraversalAction.CONTINUE
    }

    override fun visit(enumerationField: EnumerationFieldSchema): SchemaTraversalAction {
        addColumn(DataTypes.TEXT, enumerationField.multiplicity.isList())
        return SchemaTraversalAction.CONTINUE
    }

    override fun visit(associationField: AssociationFieldSchema): SchemaTraversalAction {
        val typeName = getDbAssociationTypeName(associationField)
        if (!createTypes.containsKey(typeName)) {
            createTypes[typeName] = encodeCreateDbAssociationType(keyspaceName, associationField, createTypes)
        }
        addColumn(DataTypes.custom(typeName), associationField.multiplicity.isList())
        return SchemaTraversalAction.CONTINUE
    }

    override fun visit(compositionField: CompositionFieldSchema): SchemaTraversalAction {
        if (compositionField.multiplicity.isList()) return SchemaTraversalAction.ABORT_SUB_TREE
        // TODO(deepak-nulu): visitor-flag to traverse or not traverse the resolvedEntity
        return compositionField.resolvedEntity.traverse(this)
    }
}
