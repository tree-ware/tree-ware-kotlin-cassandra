package org.tree_ware.cassandra

class TimeSlice(private val shiftBitsMillisec: Int) {
    fun getSlice(timestampMillisec: Long): Long {
        return timestampMillisec ushr shiftBitsMillisec
    }

    fun getSliceRange(startTimeMillisec: Long, endTimeMillisec: Long): LongRange {
        return LongRange(getSlice(startTimeMillisec), getSlice(endTimeMillisec))
    }
}
