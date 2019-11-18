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

/**
 * @param buckets: shiftBits of buckets, in ascending order. At least 1 bucket is required.
 */
fun getDurationBucket(buckets: List<Int>, durationMillisec: Long): Int {
    var previousBucket = buckets[0]
    var spillOver = durationMillisec ushr previousBucket
    for (i in 1 until buckets.size) {
        val bucket = buckets[i]
        spillOver = spillOver ushr (bucket - previousBucket)
        if (spillOver == 0L) break
        previousBucket = bucket
    }
    return previousBucket
}
