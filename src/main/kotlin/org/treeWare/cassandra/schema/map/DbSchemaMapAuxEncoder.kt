package org.treeWare.cassandra.schema.map

import org.treeWare.common.codec.WireFormatEncoder
import org.treeWare.model.codec.aux_encoder.AuxEncoder

private const val AUX_KEY = "db_schema_map"

class DbSchemaMapAuxEncoder : AuxEncoder {
    override fun encode(fieldName: String?, aux: Any?, wireFormatEncoder: WireFormatEncoder) {
        assert(aux is DbSchemaMapAux?)
        (aux as? DbSchemaMapAux?)?.also {
            val auxFieldName = wireFormatEncoder.getAuxFieldName(fieldName, AUX_KEY)
            wireFormatEncoder.encodeObjectStart(auxFieldName)
            wireFormatEncoder.encodeStringField("keyspace", it.keyspace)
            wireFormatEncoder.encodeStringField("table", it.table)
            wireFormatEncoder.encodeObjectEnd()
        }
    }
}
