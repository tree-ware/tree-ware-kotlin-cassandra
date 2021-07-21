package org.treeWare.cassandra.db

import com.datastax.oss.driver.api.querybuilder.BuildableQuery
import org.treeWare.cassandra.schema.map.DbSchemaMapAux
import org.treeWare.model.core.Model
import org.treeWare.model.operator.forEach

fun encodeDbModel(data: Model<Unit>, dbSchemaMap: Model<DbSchemaMapAux>): List<BuildableQuery> {
    val visitor = DbModelEncodingVisitor()
    forEach(data, dbSchemaMap, visitor)
    return visitor.commands
}
