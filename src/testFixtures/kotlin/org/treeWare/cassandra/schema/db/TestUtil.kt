package org.treeWare.cassandra.schema.db

import com.datastax.oss.driver.api.querybuilder.BuildableQuery
import java.io.StringWriter

fun List<BuildableQuery>.asString(): String {
    val cqlWriter = StringWriter()
    this.forEach {
        cqlWriter.write(it.asCql())
        cqlWriter.write(";\n")
    }
    return cqlWriter.toString()
}
