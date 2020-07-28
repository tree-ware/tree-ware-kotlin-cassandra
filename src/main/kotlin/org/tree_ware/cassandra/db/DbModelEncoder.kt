package org.tree_ware.cassandra.db

import com.datastax.oss.driver.api.querybuilder.BuildableQuery
import org.tree_ware.cassandra.schema.map.DbSchemaMapAux
import org.tree_ware.model.core.Model
import org.tree_ware.model.operator.forEach

fun encodeDbModel(data: Model<Unit>, dbSchemaMap: Model<DbSchemaMapAux>): List<BuildableQuery> {
    val visitor = DbModelEncodingVisitor()
    forEach(data, dbSchemaMap, visitor)
    return visitor.commands
}
