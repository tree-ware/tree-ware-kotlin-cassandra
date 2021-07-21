package org.treeWare.cassandra.schema.map

import org.treeWare.schema.core.Schema

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
        ),
        MutableEntityPathSchemaMap(
            pathEntities = listOf(
                MutablePathEntitySchemaMap(
                    name = "address_book",
                    keys = listOf()
                ),
                MutablePathEntitySchemaMap(
                    name = "city_info",
                    keys = listOf(
                        MutableKeyFieldSchemaMap("city/country", KeyType.PARTITION),
                        MutableKeyFieldSchemaMap("city/state", KeyType.CLUSTERING),
                        MutableKeyFieldSchemaMap("city/name", KeyType.CLUSTERING)
                    )
                )
            )
        )
    )
)
