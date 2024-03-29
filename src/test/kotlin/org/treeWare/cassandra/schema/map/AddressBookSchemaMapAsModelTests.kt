package org.treeWare.cassandra.schema.map

import org.treeWare.model.assertMatchesJson
import org.treeWare.schema.core.validate
import kotlin.test.Test
import kotlin.test.assertTrue

class AddressBookSchemaMapAsModelTests {
    @Test
    fun `Address book schema map is converted to a model correctly`() {
        val schema = newAddressBookSchema()
        val schemaErrors = validate(schema)
        assertTrue(schemaErrors.isEmpty())

        val schemaMap = newAddressBookSchemaMap(schema)
        val schemaMapErrors = validate(schemaMap)
        assertTrue(schemaMapErrors.isEmpty())

        val mapModel = asModel("test", schemaMap)
        assertMatchesJson(mapModel, DbSchemaMapAuxEncoder(), "map/address_book_db_schema_map_model.json")
    }
}
