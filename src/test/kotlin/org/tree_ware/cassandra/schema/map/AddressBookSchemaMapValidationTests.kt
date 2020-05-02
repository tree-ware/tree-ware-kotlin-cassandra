package org.tree_ware.cassandra.schema.map

import org.tree_ware.schema.core.validate
import kotlin.test.Test
import kotlin.test.assertTrue

class AddressBookSchemaMapValidationTests {
    @Test
    fun `Address book schema map is valid`() {
        val schema = newAddressBookSchema()
        val schemaErrors = validate(schema)
        assertTrue(schemaErrors.isEmpty())

        val schemaMap = newAddressBookSchemaMap(schema)
        val errors = validate(schemaMap)
        assertTrue(errors.isEmpty())
    }
}
