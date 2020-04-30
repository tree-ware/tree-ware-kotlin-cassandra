package org.tree_ware.cassandra.schema.map

fun newAddressBookSchemaMap() = MutableSchemaMap(
    schema = newAddressBookSchema(),
    entities = listOf(
        MutableEntitySchemaMap(
            pathKeys = listOf(
                MutableEntityKeysSchemaMap(
                    name = "address_book",
                    keys = listOf()
                )
            )
        ),
        MutableEntitySchemaMap(
            pathKeys = listOf(
                MutableEntityKeysSchemaMap(
                    name = "address_book",
                    keys = listOf()
                ),
                MutableEntityKeysSchemaMap(
                    name = "person",
                    keys = listOf(
                        MutableKeyFieldSchemaMap("id", KeyType.CLUSTERING)
                    )
                )
            ),
            usePartitionId = 1
        ),
        MutableEntitySchemaMap(
            pathKeys = listOf(
                MutableEntityKeysSchemaMap(
                    name = "address_book",
                    keys = listOf()
                ),
                MutableEntityKeysSchemaMap(
                    name = "person",
                    keys = listOf(
                        MutableKeyFieldSchemaMap("id", KeyType.PARTITION)
                    )
                ),
                MutableEntityKeysSchemaMap(
                    name = "relation",
                    keys = listOf(
                        MutableKeyFieldSchemaMap("id", KeyType.CLUSTERING)
                    )
                )
            )
        )
    )
)
