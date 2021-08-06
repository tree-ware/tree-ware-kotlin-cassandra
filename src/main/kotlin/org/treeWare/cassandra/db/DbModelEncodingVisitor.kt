package org.treeWare.cassandra.db

import com.datastax.oss.driver.api.querybuilder.BuildableQuery
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.*
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert
import com.datastax.oss.driver.api.querybuilder.term.Term
import org.treeWare.cassandra.schema.map.DbSchemaMapAux
import org.treeWare.common.traversal.TraversalAction
import org.treeWare.model.action.isCompositionField
import org.treeWare.model.action.isCompositionListField
import org.treeWare.model.core.*
import org.treeWare.model.operator.Leader1Follower1ModelVisitor
import org.treeWare.model.operator.dispatchVisit
import org.treeWare.schema.core.EntitySchema
import java.util.*

private data class KeyColumn(val name: String, val value: Term)
private data class EntityTable(val name: String, val keys: List<KeyColumn>)

class DbModelEncodingVisitor : Leader1Follower1ModelVisitor<Unit, DbSchemaMapAux, TraversalAction> {
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
            entityTable.keys.forEach { (name, value) ->
                updatedRow = updatedRow.value(name, value)
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

    private fun addNonListField(field: FieldModel<Unit>): TraversalAction {
        val fieldValue = (field as? SingleFieldModel<Unit>)?.value
        val value = fieldValue?.let { dispatchVisit(it, valueEncodingVisitor) } ?: ""
        if (!field.schema.isKey) addColumn(getColumnName(field), raw(value))
        return TraversalAction.CONTINUE
    }

    private fun addListField(
        listField: ListFieldModel<Unit>,
        values: List<ElementModel<Unit>>
    ): TraversalAction {
        val value = values.joinToString(",", "{", "}") { dispatchVisit(it, valueEncodingVisitor) ?: "" }
        addColumn(getColumnName(listField), raw(value))
        return TraversalAction.ABORT_SUB_TREE
    }

    override fun visit(leaderModel1: Model<Unit>, followerModel1: Model<DbSchemaMapAux>?): TraversalAction =
        TraversalAction.CONTINUE

    override fun leave(leaderModel1: Model<Unit>, followerModel1: Model<DbSchemaMapAux>?) {}

    override fun visit(leaderRoot1: RootModel<Unit>, followerRoot1: RootModel<DbSchemaMapAux>?): TraversalAction {
        addRow(leaderRoot1.schema.name, leaderRoot1, leaderRoot1.schema.resolvedEntity, followerRoot1?.aux)
        return TraversalAction.CONTINUE
    }

    override fun leave(leaderRoot1: RootModel<Unit>, followerRoot1: RootModel<DbSchemaMapAux>?) {
        rowDone(followerRoot1?.aux)
    }

    override fun visit(
        leaderEntity1: EntityModel<Unit>,
        followerEntity1: EntityModel<DbSchemaMapAux>?
    ): TraversalAction {
        if (isCompositionListField(leaderEntity1.parent)) {
            val dbSchemaMap = followerEntity1?.aux ?: return TraversalAction.ABORT_SUB_TREE
            addRow(leaderEntity1.parent.schema.name, leaderEntity1, leaderEntity1.schema, dbSchemaMap)
        }
        return TraversalAction.CONTINUE
    }

    override fun leave(leaderEntity1: EntityModel<Unit>, followerEntity1: EntityModel<DbSchemaMapAux>?) {
        if (isCompositionListField(leaderEntity1.parent)) {
            val dbSchemaMap = followerEntity1?.aux ?: return
            rowDone(dbSchemaMap)
        }
    }

    // Fields

    override fun visit(
        leaderField1: SingleFieldModel<Unit>,
        followerField1: SingleFieldModel<DbSchemaMapAux>?
    ): TraversalAction =
        if (isCompositionField(leaderField1)) {
            columnNamePrefixes.addLast(leaderField1.schema.name)
            TraversalAction.CONTINUE
        } else addNonListField(leaderField1)

    override fun leave(leaderField1: SingleFieldModel<Unit>, followerField1: SingleFieldModel<DbSchemaMapAux>?) {
        if (isCompositionField(leaderField1)) columnNamePrefixes.pollLast()
    }

    override fun visit(
        leaderField1: ListFieldModel<Unit>,
        followerField1: ListFieldModel<DbSchemaMapAux>?
    ): TraversalAction =
        if (isCompositionListField(leaderField1)) TraversalAction.CONTINUE
        else addListField(leaderField1, leaderField1.values)

    override fun leave(
        leaderField1: ListFieldModel<Unit>,
        followerField1: ListFieldModel<DbSchemaMapAux>?
    ) {
    }

    // Values

    override fun visit(
        leaderValue1: PrimitiveModel<Unit>,
        followerValue1: PrimitiveModel<DbSchemaMapAux>?
    ): TraversalAction = TraversalAction.CONTINUE

    override fun leave(leaderValue1: PrimitiveModel<Unit>, followerValue1: PrimitiveModel<DbSchemaMapAux>?) {}

    override fun visit(
        leaderValue1: AliasModel<Unit>,
        followerValue1: AliasModel<DbSchemaMapAux>?
    ): TraversalAction = TraversalAction.CONTINUE

    override fun leave(leaderValue1: AliasModel<Unit>, followerValue1: AliasModel<DbSchemaMapAux>?) {}

    override fun visit(
        leaderValue1: EnumerationModel<Unit>,
        followerValue1: EnumerationModel<DbSchemaMapAux>?
    ): TraversalAction = TraversalAction.CONTINUE

    override fun leave(
        leaderValue1: EnumerationModel<Unit>,
        followerValue1: EnumerationModel<DbSchemaMapAux>?
    ) {
    }

    override fun visit(
        leaderValue1: AssociationModel<Unit>,
        followerValue1: AssociationModel<DbSchemaMapAux>?
    ): TraversalAction = TraversalAction.CONTINUE

    override fun leave(
        leaderValue1: AssociationModel<Unit>,
        followerValue1: AssociationModel<DbSchemaMapAux>?
    ) {
    }

    // Sub-values

    override fun visit(
        leaderEntityKeys1: EntityKeysModel<Unit>,
        followerEntityKeys1: EntityKeysModel<DbSchemaMapAux>?
    ): TraversalAction = TraversalAction.CONTINUE

    override fun leave(
        leaderEntityKeys1: EntityKeysModel<Unit>,
        followerEntityKeys1: EntityKeysModel<DbSchemaMapAux>?
    ) {
    }
}
