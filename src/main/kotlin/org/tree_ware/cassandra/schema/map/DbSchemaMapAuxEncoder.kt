package org.tree_ware.cassandra.schema.map

import org.tree_ware.common.codec.WireFormatEncoder
import org.tree_ware.model.codec.aux_encoder.AuxEncoder

private const val AUX_KEY = "db_schema_map"

class DbSchemaMapAuxEncoder : AuxEncoder {
    override fun encode(aux: Any?, wireFormatEncoder: WireFormatEncoder) {
        assert(aux is DbSchemaMapAux?)
        (aux as? DbSchemaMapAux?)?.also {
            wireFormatEncoder.encodeObjectStart(AUX_KEY)
            wireFormatEncoder.encodeStringField("keyspace", it.keyspace)
            wireFormatEncoder.encodeStringField("table", it.table)
            wireFormatEncoder.encodeObjectEnd()
        }
    }
}
