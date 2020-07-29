package org.tree_ware.cassandra.db

import com.datastax.oss.driver.api.querybuilder.BuildableQuery
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.*
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert
import com.datastax.oss.driver.api.querybuilder.term.Term
import org.tree_ware.cassandra.schema.map.DbSchemaMapAux
import org.tree_ware.model.core.*
import org.tree_ware.model.operator.Leader1Follower1ModelVisitor
import org.tree_ware.schema.core.EntitySchema
import org.tree_ware.schema.core.SchemaTraversalAction
import java.util.*

private data class KeyColumn(val name: String, val value: Term)
private data class EntityTable(val name: String, val keys: List<KeyColumn>)

class DbModelEncodingVisitor : Leader1Follower1ModelVisitor<Unit, DbSchemaMapAux, SchemaTraversalAction> {
    val commands: List<BuildableQuery> get() = _commands
    private val _commands: MutableList<BuildableQuery> = mutableListOf()

    private val entityPathTables = ArrayDeque<EntityTable>()
    private val ongoingRowStack = ArrayDeque<RegularInsert>()
    private val columnNamePrefixes = ArrayDeque<String>()
    private val valueEncodingVisitor = DbValueEncodingVisitor()

    private fun addEntityToPath(
        entityName: String,
        leaderEntity: BaseEntityModel<Unit>,
        leaderEntitySchema: EntitySchema
    ) {
        val keys = leaderEntitySchema.fields.filter { it.isKey }.flatMap { keySchema ->
            val keyField = leaderEntity.getField(keySchema.name) ?: return@flatMap listOf<KeyColumn>()
            getKeyValueList(entityName, keyField, valueEncodingVisitor) { key, value -> KeyColumn(key, raw(value)) }
        }
        // TODO(deepak-nulu): error if all keys are not present; reject entire request or only this subtree?
        entityPathTables.addLast(EntityTable(entityName, keys))
    }

    private fun addKeysToRow(row: RegularInsert): RegularInsert {
        var updatedRow = row
        entityPathTables.forEach { entityTable ->
            entityTable.keys.forEach { keyColumn ->
                updatedRow = updatedRow.value(keyColumn.name, keyColumn.value)
            }
        }
        return updatedRow
    }

    private fun addRow(
        name: String,
        leaderEntity: BaseEntityModel<Unit>,
        leaderEntitySchema: EntitySchema,
        dbSchemaMap: DbSchemaMapAux?
    ) {
        assert(dbSchemaMap != null)
        if (dbSchemaMap == null) return
        addEntityToPath(name, leaderEntity, leaderEntitySchema)
        // Create a DB row for the entity
        val row = insertInto(dbSchemaMap.keyspace, dbSchemaMap.table).value(SYNTHETIC_PART_ID_NAME, literal(0))
        val rowWithKeys = addKeysToRow(row)
        ongoingRowStack.addFirst(rowWithKeys)
    }

    private fun rowDone(dbSchemaMap: DbSchemaMapAux?) {
        assert(dbSchemaMap != null)
        if (dbSchemaMap == null) return
        entityPathTables.pollLast()
        // Transfer completed row from ongoingStack to _commands
        assert(ongoingRowStack.isNotEmpty())
        val currentRow = ongoingRowStack.pollFirst()
        // TODO(deepak-nulu): compute and set the partition-id
        _commands.add(currentRow)
    }

    private fun addColumn(name: String, value: Term) {
        assert(ongoingRowStack.isNotEmpty())
        val currentRow = ongoingRowStack.pollFirst()
        val updatedRow = currentRow.value(name, value)
        ongoingRowStack.addFirst(updatedRow)
    }

    private fun getColumnName(field: FieldModel<Unit>): String =
        if (columnNamePrefixes.isEmpty()) field.schema.name
        else "\"${columnNamePrefixes.joinToString("/")}/${field.schema.name}\""

    private fun addNonListField(field: FieldModel<Unit>): SchemaTraversalAction {
        val value = field.dispatch(valueEncodingVisitor)
        if (!field.schema.isKey) addColumn(getColumnName(field), raw(value))
        return SchemaTraversalAction.CONTINUE
    }

    private fun addListField(
        listField: ListFieldModel<Unit>,
        values: List<FieldModel<Unit>>
    ): SchemaTraversalAction {
        val value = values.joinToString(",", "{", "}") { it.dispatch(valueEncodingVisitor) }
        addColumn(getColumnName(listField), raw(value))
        return SchemaTraversalAction.ABORT_SUB_TREE
    }

    override fun visit(leaderModel1: Model<Unit>, followerModel1: Model<DbSchemaMapAux>?): SchemaTraversalAction =
        SchemaTraversalAction.CONTINUE

    override fun leave(leaderModel1: Model<Unit>, followerModel1: Model<DbSchemaMapAux>?) {}

    override fun visit(leaderRoot1: RootModel<Unit>, followerRoot1: RootModel<DbSchemaMapAux>?): SchemaTraversalAction {
        addRow(leaderRoot1.schema.name, leaderRoot1, leaderRoot1.schema.resolvedEntity, followerRoot1?.aux)
        return SchemaTraversalAction.CONTINUE
    }

    override fun leave(leaderRoot1: RootModel<Unit>, followerRoot1: RootModel<DbSchemaMapAux>?) {
        rowDone(followerRoot1?.aux)
    }

    override fun visit(
        leaderEntity1: EntityModel<Unit>,
        followerEntity1: EntityModel<DbSchemaMapAux>?
    ): SchemaTraversalAction {
        if (leaderEntity1.parent is CompositionListFieldModel) {
            val dbSchemaMap = followerEntity1?.aux ?: return SchemaTraversalAction.ABORT_SUB_TREE
            addRow(leaderEntity1.parent.schema.name, leaderEntity1, leaderEntity1.schema, dbSchemaMap)
        }
        return SchemaTraversalAction.CONTINUE
    }

    override fun leave(leaderEntity1: EntityModel<Unit>, followerEntity1: EntityModel<DbSchemaMapAux>?) {
        if (leaderEntity1.parent is CompositionListFieldModel) {
            val dbSchemaMap = followerEntity1?.aux ?: return
            rowDone(dbSchemaMap)
        }
    }

    override fun visit(
        leaderField1: PrimitiveFieldModel<Unit>,
        followerField1: PrimitiveFieldModel<DbSchemaMapAux>?
    ): SchemaTraversalAction {
        return addNonListField(leaderField1)
    }

    override fun leave(leaderField1: PrimitiveFieldModel<Unit>, followerField1: PrimitiveFieldModel<DbSchemaMapAux>?) {}

    override fun visit(
        leaderField1: AliasFieldModel<Unit>,
        followerField1: AliasFieldModel<DbSchemaMapAux>?
    ): SchemaTraversalAction {
        return addNonListField(leaderField1)
    }

    override fun leave(leaderField1: AliasFieldModel<Unit>, followerField1: AliasFieldModel<DbSchemaMapAux>?) {}

    override fun visit(
        leaderField1: EnumerationFieldModel<Unit>,
        followerField1: EnumerationFieldModel<DbSchemaMapAux>?
    ): SchemaTraversalAction {
        return addNonListField(leaderField1)
    }

    override fun leave(
        leaderField1: EnumerationFieldModel<Unit>,
        followerField1: EnumerationFieldModel<DbSchemaMapAux>?
    ) {
    }

    override fun visit(
        leaderField1: AssociationFieldModel<Unit>,
        followerField1: AssociationFieldModel<DbSchemaMapAux>?
    ): SchemaTraversalAction {
        return addNonListField(leaderField1)
    }

    override fun leave(
        leaderField1: AssociationFieldModel<Unit>,
        followerField1: AssociationFieldModel<DbSchemaMapAux>?
    ) {
    }

    override fun visit(
        leaderField1: CompositionFieldModel<Unit>,
        followerField1: CompositionFieldModel<DbSchemaMapAux>?
    ): SchemaTraversalAction {
        columnNamePrefixes.addLast(leaderField1.schema.name)
        return SchemaTraversalAction.CONTINUE
    }

    override fun leave(
        leaderField1: CompositionFieldModel<Unit>,
        followerField1: CompositionFieldModel<DbSchemaMapAux>?
    ) {
        columnNamePrefixes.pollLast()
    }

    override fun visit(
        leaderField1: PrimitiveListFieldModel<Unit>,
        followerField1: PrimitiveListFieldModel<DbSchemaMapAux>?
    ): SchemaTraversalAction {
        return addListField(leaderField1, leaderField1.primitives)
    }

    override fun leave(
        leaderField1: PrimitiveListFieldModel<Unit>,
        followerField1: PrimitiveListFieldModel<DbSchemaMapAux>?
    ) {
    }

    override fun visit(
        leaderField1: AliasListFieldModel<Unit>,
        followerField1: AliasListFieldModel<DbSchemaMapAux>?
    ): SchemaTraversalAction {
        return addListField(leaderField1, leaderField1.aliases)
    }

    override fun leave(leaderField1: AliasListFieldModel<Unit>, followerField1: AliasListFieldModel<DbSchemaMapAux>?) {}

    override fun visit(
        leaderField1: EnumerationListFieldModel<Unit>,
        followerField1: EnumerationListFieldModel<DbSchemaMapAux>?
    ): SchemaTraversalAction {
        return addListField(leaderField1, leaderField1.enumerations)
    }

    override fun leave(
        leaderField1: EnumerationListFieldModel<Unit>,
        followerField1: EnumerationListFieldModel<DbSchemaMapAux>?
    ) {
    }

    override fun visit(
        leaderField1: AssociationListFieldModel<Unit>,
        followerField1: AssociationListFieldModel<DbSchemaMapAux>?
    ): SchemaTraversalAction {
        return addListField(leaderField1, leaderField1.associations)
    }

    override fun leave(
        leaderField1: AssociationListFieldModel<Unit>,
        followerField1: AssociationListFieldModel<DbSchemaMapAux>?
    ) {
    }

    override fun visit(
        leaderField1: CompositionListFieldModel<Unit>,
        followerField1: CompositionListFieldModel<DbSchemaMapAux>?
    ): SchemaTraversalAction = SchemaTraversalAction.CONTINUE

    override fun leave(
        leaderField1: CompositionListFieldModel<Unit>,
        followerField1: CompositionListFieldModel<DbSchemaMapAux>?
    ) {
    }
}
