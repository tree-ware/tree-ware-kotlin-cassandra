package org.tree_ware.cassandra.db

import com.datastax.oss.driver.api.core.type.DataType
import com.datastax.oss.driver.api.core.type.DataTypes
import org.tree_ware.schema.core.*

fun treeWareToCqlDataType(primitive: PrimitiveSchema): DataType = when (primitive) {
    is BooleanSchema -> DataTypes.BOOLEAN
    is ByteSchema -> DataTypes.TINYINT
    is ShortSchema -> DataTypes.SMALLINT
    is IntSchema -> DataTypes.INT
    is LongSchema -> DataTypes.BIGINT
    is FloatSchema -> DataTypes.FLOAT
    is DoubleSchema -> DataTypes.DOUBLE
    is StringSchema -> DataTypes.TEXT
    is Password1WaySchema -> DataTypes.TEXT
    is Password2WaySchema -> DataTypes.TEXT
    is UuidSchema -> DataTypes.UUID
    is BlobSchema -> DataTypes.BLOB
    is TimestampSchema -> DataTypes.TIMESTAMP
    else -> DataTypes.TEXT
}
