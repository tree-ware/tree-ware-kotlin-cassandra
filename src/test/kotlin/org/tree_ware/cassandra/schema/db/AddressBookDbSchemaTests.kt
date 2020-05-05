package org.tree_ware.cassandra.schema.db

import org.apache.logging.log4j.LogManager
import org.tree_ware.cassandra.db.encodeDbSchema
import org.tree_ware.cassandra.schema.map.newAddressBookSchema
import org.tree_ware.cassandra.schema.map.newAddressBookSchemaMap
import org.tree_ware.schema.core.validate
import java.io.File
import java.io.StringWriter
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class AddressBookDbSchemaTests {
    val logger = LogManager.getLogger()

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

        val dbCommands = encodeDbSchema(schemaMap)
        val cqlWriter = StringWriter()
        dbCommands.forEach {
            cqlWriter.write(it.asCql())
            cqlWriter.write("\n")
        }
        val actual = cqlWriter.toString()

        assertEquals(expected, actual)
    }
}
