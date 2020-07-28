package org.tree_ware.cassandra.schema.db

import org.tree_ware.cassandra.db.encodeDbModel
import org.tree_ware.cassandra.schema.map.asModel
import org.tree_ware.cassandra.schema.map.newAddressBookSchema
import org.tree_ware.cassandra.schema.map.newAddressBookSchemaMap
import org.tree_ware.cassandra.schema.map.validate
import org.tree_ware.model.getModel
import org.tree_ware.schema.core.validate
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
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
        val data = getModel<Unit>(schema, "model/address_book_1.json")

        val cqlFile = File("src/test/resources/db/address_book_db_data_cql.txt")
        assertTrue(cqlFile.exists())
        val expected = cqlFile.readText()

        val dbCommands = encodeDbModel(data, dbSchemaMap)
        val actual = dbCommands.asString()

        assertEquals(expected, actual)
    }
}
