package org.treeWare.cassandra.schema.map

import org.treeWare.common.codec.AbstractDecodingStateMachine
import org.treeWare.model.codec.decoder.stateMachine.AuxDecodingStateMachine
import org.treeWare.model.codec.decoder.stateMachine.DecodingStack

class DbSchemaMapAuxStateMachine(
    private val stack: DecodingStack
) : AuxDecodingStateMachine<DbSchemaMapAux>, AbstractDecodingStateMachine(true) {
    private var aux: DbSchemaMapAux? = null

    override fun newAux() {
        aux = DbSchemaMapAux("", "")
    }

    override fun getAux(): DbSchemaMapAux? {
        return aux
    }

    override fun decodeStringValue(value: String): Boolean {
        when (keyName) {
            "keyspace" -> aux?.keyspace = value
            "table" -> aux?.table = value
        }
        return true
    }

    override fun decodeObjectStart(): Boolean {
        return true
    }

    override fun decodeObjectEnd(): Boolean {
        // Remove self from stack
        stack.pollFirst()
        return true
    }

    override fun decodeListStart(): Boolean {
        // This method should never get called
        assert(false)
        return false
    }

    override fun decodeListEnd(): Boolean {
        // This method should never get called
        assert(false)
        return false
    }
}
