package org.treeWare.cassandra.schema.db

import org.treeWare.cassandra.db.encodeDbModel
import org.treeWare.cassandra.schema.map.asModel
import org.treeWare.cassandra.schema.map.newAddressBookSchema
import org.treeWare.cassandra.schema.map.newAddressBookSchemaMap
import org.treeWare.cassandra.schema.map.validate
import org.treeWare.model.assertMatchesJson
import org.treeWare.model.getFileReader
import org.treeWare.model.getModel
import org.treeWare.schema.core.validate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

const val ENVIRONMENT = "test"

class AddressBookDbModelEncoderTests {
    @Test
    fun `Address book model is encoded into DB commands`() {
        val schema = newAddressBookSchema()
        val schemaErrors = validate(schema)
        assertTrue(schemaErrors.isEmpty())

        val schemaMap = newAddressBookSchemaMap(schema)
        val schemaMapErrors = validate(schemaMap)
        assertTrue(schemaMapErrors.isEmpty())

        val dbSchemaMap = asModel(ENVIRONMENT, schemaMap)
        val data = getModel<Unit>(schema, "db/address_book_write_request.json")
        assertMatchesJson(data, null, "db/address_book_write_request.json")

        val cqlFileReader = getFileReader("db/address_book_db_data_cql.txt")
        assertNotNull(cqlFileReader)
        val expected = cqlFileReader.readText()

        val dbCommands = encodeDbModel(data, dbSchemaMap)
        val actual = dbCommands.asString()

        assertEquals(expected, actual)
    }
}
