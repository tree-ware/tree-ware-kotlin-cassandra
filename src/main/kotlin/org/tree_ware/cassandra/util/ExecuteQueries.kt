package org.tree_ware.cassandra.util

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.querybuilder.BuildableQuery
import kotlinx.coroutines.future.await

suspend fun executeQueries(cqlSession: CqlSession, queries: List<BuildableQuery>) {
    queries.forEach { query ->
        cqlSession.executeAsync(query.build()).await()
    }
}
