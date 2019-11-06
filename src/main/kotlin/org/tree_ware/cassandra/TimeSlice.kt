package org.tree_ware.cassandra

fun getTimeSlice(shiftBitsMillisec: Int, timestampMillisec: Long): Long {
    return timestampMillisec ushr shiftBitsMillisec
}

fun getTimeSliceRange(shiftBitsMillisec: Int, startTimeMillisec: Long, endTimeMillisec: Long): LongRange {
    return LongRange(
        getTimeSlice(shiftBitsMillisec, startTimeMillisec),
        getTimeSlice(shiftBitsMillisec, endTimeMillisec)
    )
}

@Deprecated(message= "Use equivalent top-level functions instead of this class")
class TimeSlice(private val shiftBitsMillisec: Int) {
    @Deprecated(
        message = "Use equivalent top-level function",
        replaceWith = ReplaceWith(
            expression = "getTimeSlice(shiftBitsMillisec, timestampMillisec)",
            imports = ["org.tree_ware.cassandra.getTimeSlice"]
        ))
    fun getSlice(timestampMillisec: Long): Long {
        return timestampMillisec ushr shiftBitsMillisec
    }

    @Deprecated(
        message = "Use equivalent top-level function",
        replaceWith = ReplaceWith(
            expression = "getTimeSliceRange(shiftBitsMillisec, startTimeMillisec, endTimeMillisec)",
            imports = ["org.tree_ware.cassandra.getTimeSliceRange"]
        ))
    fun getSliceRange(startTimeMillisec: Long, endTimeMillisec: Long): LongRange {
        return LongRange(getSlice(startTimeMillisec), getSlice(endTimeMillisec))
    }
}
