package org.tree_ware.cassandra.schema.map

import org.tree_ware.model.codec.encodeJson
import org.tree_ware.model.getFileReader
import org.tree_ware.schema.core.validate
import java.io.StringWriter
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
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

        val expectedFileReader = getFileReader("map/address_book_db_schema_map_model.json")
        assertNotNull(expectedFileReader)
        val expected = expectedFileReader.readText()

        val actual = jsonWriter.toString()
        assertEquals(expected, actual)
    }
}
