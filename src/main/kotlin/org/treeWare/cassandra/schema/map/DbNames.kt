package org.treeWare.cassandra.schema.map

fun getKeyspaceName(environment: String, root: RootSchemaMap) = "${environment}_${root.keyspaceName}"
