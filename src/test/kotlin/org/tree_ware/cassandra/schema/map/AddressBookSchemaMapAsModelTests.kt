package org.tree_ware.cassandra.schema.map

import org.tree_ware.model.codec.encodeJson
import java.io.File
import java.io.StringWriter
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class AddressBookSchemaMapAsModelTests {
    @Test
    fun `Address book schema map is converted to a model correctly`() {
        val schema = newAddressBookSchema()
        val schemaErrors = org.tree_ware.schema.core.validate(schema)
        assertTrue(schemaErrors.isEmpty())

        val schemaMap = newAddressBookSchemaMap(schema)
        val errors = validate(schemaMap)
        assertTrue(errors.isEmpty())

        val mapModel = asModel("test", schemaMap)

        // Encode the map model to JSON and compare with expected JSON
        val jsonWriter = StringWriter()
        val isEncoded = try {
            encodeJson(mapModel, DbSchemaMapAuxEncoder(), jsonWriter, true)
        } catch (e: Throwable) {
            e.printStackTrace()
            println("Encoded so far:")
            println(jsonWriter.toString())
            println("End of encoded")
            false
        }
        assertTrue(isEncoded)

        val expectedFile = File("src/test/resources/map/address_book_db_schema_map_model.json")
        assertTrue(expectedFile.exists())
        val expected = expectedFile.readText()

        val actual = jsonWriter.toString()
        assertEquals(expected, actual)
    }
}
