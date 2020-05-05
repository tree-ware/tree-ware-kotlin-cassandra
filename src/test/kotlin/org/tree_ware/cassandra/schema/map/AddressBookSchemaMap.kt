package org.tree_ware.cassandra.schema.map

import org.tree_ware.schema.core.Schema

fun newAddressBookSchemaMap(schema: Schema) = MutableSchemaMap(
    schema = schema,
    entityPaths = listOf(
        MutableEntityPathSchemaMap(
            pathEntities = listOf(
                MutablePathEntitySchemaMap(
                    name = "address_book",
                    keys = listOf()
                )
            )
        ),
        MutableEntityPathSchemaMap(
            pathEntities = listOf(
                MutablePathEntitySchemaMap(
                    name = "address_book",
                    keys = listOf()
                ),
                MutablePathEntitySchemaMap(
                    name = "person",
                    keys = listOf(
                        MutableKeyFieldSchemaMap("id", KeyType.CLUSTERING)
                    )
                )
            )
        ),
        MutableEntityPathSchemaMap(
            pathEntities = listOf(
                MutablePathEntitySchemaMap(
                    name = "address_book",
                    keys = listOf()
                ),
                MutablePathEntitySchemaMap(
                    name = "person",
                    keys = listOf(
                        MutableKeyFieldSchemaMap("id", KeyType.PARTITION)
                    )
                ),
                MutablePathEntitySchemaMap(
                    name = "relation",
                    keys = listOf(
                        MutableKeyFieldSchemaMap("id", KeyType.CLUSTERING)
                    )
                )
            )
        )
    )
)
