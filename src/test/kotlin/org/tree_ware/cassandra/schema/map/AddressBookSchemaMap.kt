package org.tree_ware.cassandra.schema.map

import org.tree_ware.schema.core.Schema

fun newAddressBookSchemaMap(schema: Schema) = MutableSchemaMap(
    schema = schema,
    entityMaps = listOf(
        MutableEntitySchemaMap(
            pathKeyMaps = listOf(
                MutableEntityKeysSchemaMap(
                    name = "address_book",
                    keyFieldMaps = listOf()
                )
            )
        ),
        MutableEntitySchemaMap(
            pathKeyMaps = listOf(
                MutableEntityKeysSchemaMap(
                    name = "address_book",
                    keyFieldMaps = listOf()
                ),
                MutableEntityKeysSchemaMap(
                    name = "person",
                    keyFieldMaps = listOf(
                        MutableKeyFieldSchemaMap("id", KeyType.CLUSTERING)
                    )
                )
            ),
            usePartitionId = 1
        ),
        MutableEntitySchemaMap(
            pathKeyMaps = listOf(
                MutableEntityKeysSchemaMap(
                    name = "address_book",
                    keyFieldMaps = listOf()
                ),
                MutableEntityKeysSchemaMap(
                    name = "person",
                    keyFieldMaps = listOf(
                        MutableKeyFieldSchemaMap("id", KeyType.PARTITION)
                    )
                ),
                MutableEntityKeysSchemaMap(
                    name = "relation",
                    keyFieldMaps = listOf(
                        MutableKeyFieldSchemaMap("id", KeyType.CLUSTERING)
                    )
                )
            )
        )
    )
)
