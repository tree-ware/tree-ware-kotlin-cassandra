package org.tree_ware.cassandra.schema.db

import org.tree_ware.cassandra.db.encodeCreateDbSchema
import org.tree_ware.cassandra.schema.map.newAddressBookSchema
import org.tree_ware.cassandra.schema.map.newAddressBookSchemaMap
import org.tree_ware.model.getFileReader
import org.tree_ware.schema.core.validate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class AddressBookDbSchemaTests {
    @Test
    fun `Address book DB schema is valid`() {
        val schema = newAddressBookSchema()
        val schemaErrors = validate(schema)
        assertTrue(schemaErrors.isEmpty())

        val schemaMap = newAddressBookSchemaMap(schema)
        val errors = org.tree_ware.cassandra.schema.map.validate(schemaMap)
        assertTrue(errors.isEmpty())

        val cqlFileReader = getFileReader("db/address_book_db_schema_cql.txt")
        assertNotNull(cqlFileReader)
        val expected = cqlFileReader.readText()

        val dbCommands = encodeCreateDbSchema("test", schemaMap)
        val actual = dbCommands.asString()

        assertEquals(expected, actual)
    }
}
