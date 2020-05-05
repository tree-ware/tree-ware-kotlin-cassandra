package org.tree_ware.cassandra.db

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.type.DataType
import com.datastax.oss.driver.api.core.type.DataTypes
import org.tree_ware.cassandra.schema.map.KeyType
import org.tree_ware.schema.core.*
import org.tree_ware.schema.visitor.AbstractSchemaVisitor

data class ColumnSchema(val name: CqlIdentifier, val keyType: KeyType?, val dataType: DataType)

class DbColumnSchemaGeneratingVisitor : AbstractSchemaVisitor(), BracketedVisitor {
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

    // BracketedVisitor methods

    override fun objectStart(name: String) {}

    override fun objectEnd() {
        nameParts.removeAt(nameParts.lastIndex)
    }

    override fun listStart(name: String) {}

    override fun listEnd() {}

    // SchemaVisitor methods

    override fun visit(namedElement: NamedElementSchema): Boolean {
        nameParts.add(namedElement.name)
        return true
    }

    // Fields

    override fun visit(primitiveField: PrimitiveFieldSchema): Boolean {
        addColumn(treeWareToCqlDataType(primitiveField.primitive), primitiveField.multiplicity.isList())
        return true
    }

    override fun visit(aliasField: AliasFieldSchema): Boolean {
        addColumn(treeWareToCqlDataType(aliasField.resolvedAlias.primitive), aliasField.multiplicity.isList())
        return true
    }

    override fun visit(enumerationField: EnumerationFieldSchema): Boolean {
        addColumn(DataTypes.TEXT, enumerationField.multiplicity.isList())
        return true
    }

    override fun visit(associationField: AssociationFieldSchema): Boolean {
        // TODO(deepak-nulu): use a user-defined-type for associations
        addColumn(DataTypes.TEXT, associationField.multiplicity.isList())
        return true
    }

    override fun visit(compositionField: CompositionFieldSchema): Boolean {
        // TODO(deepak-nulu): ability for visit() method to abort entire traversal or just its subtree
        if (compositionField.multiplicity.isList()) return true
        // TODO(deepak-nulu): visitor-flag to traverse or not traverse the resolvedEntity
        return compositionField.resolvedEntity.accept(this)
    }
}
