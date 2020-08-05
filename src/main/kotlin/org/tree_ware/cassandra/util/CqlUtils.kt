package org.tree_ware.cassandra.util

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.querybuilder.select.Selector

private val SYSTEM_KEYSPACE = CqlIdentifier.fromCql("system")
private val SUM_FUNCTION = CqlIdentifier.fromCql("sum")

fun getSumColumnSelector(column: CqlIdentifier): Selector {
    return Selector.function(SYSTEM_KEYSPACE, SUM_FUNCTION, Selector.column(column))
}

fun Selector.selectorAsString(): String {
    val stringBuilder = StringBuilder()
    this.appendTo(stringBuilder)
    return stringBuilder.toString()
}
