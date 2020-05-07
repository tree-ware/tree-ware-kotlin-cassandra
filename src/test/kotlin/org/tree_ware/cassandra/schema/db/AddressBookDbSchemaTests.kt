package org.tree_ware.cassandra.schema.db

import org.tree_ware.cassandra.db.encodeCreateDbSchema
import org.tree_ware.cassandra.schema.map.newAddressBookSchema
import org.tree_ware.cassandra.schema.map.newAddressBookSchemaMap
import org.tree_ware.schema.core.validate
import java.io.File
import java.io.StringWriter
import kotlin.test.Test
import kotlin.test.assertEquals
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

        val cqlFile = File("src/test/resources/db_schema/address_book_db_schema_cql.txt")
        assertTrue(cqlFile.exists())
        val expected = cqlFile.readText()

        val dbCommands = encodeCreateDbSchema("test", schemaMap)
        val cqlWriter = StringWriter()
        dbCommands.forEach {
            cqlWriter.write(it.asCql())
            cqlWriter.write(";\n")
        }
        val actual = cqlWriter.toString()

        assertEquals(expected, actual)
    }
}
