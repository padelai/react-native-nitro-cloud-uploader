package com.margelo.nitro.nitroclouduploader

/**
 * Tracks completed bytes plus per-part in-flight bytes so `upload-progress`
 * events never move backward when parallel parts finish out of order or a
 * retry resets a part's in-flight count.
 *
 * Pure (no Android/OkHttp deps) and internally synchronized — driven
 * concurrently from each part's `writeTo` thread, the completion path, and the
 * periodic emitter coroutine.
 */
class ProgressAccumulator(val totalBytes: Long) {
    private val inFlight = HashMap<Int, Long>()
    private var completedBytes = 0L
    // Starts at 0 (not -1) so nextEmit() suppresses a redundant 0-byte event
    // before any bytes have been sent — upload-started already covers that state.
    private var lastEmittedBytes = 0L

    /** Bytes streamed so far for [part] on the current attempt (absolute, not a delta). */
    @Synchronized
    fun onBytes(part: Int, sentSoFar: Long) {
        inFlight[part] = sentSoFar
    }

    /** Fold a completed part's full size into the running total; drop its in-flight entry. */
    @Synchronized
    fun onComplete(part: Int, size: Long) {
        completedBytes += size
        inFlight.remove(part)
    }

    /** Drop an abandoned attempt (failure / cancel / retry) without crediting its bytes. */
    @Synchronized
    fun onDrop(part: Int) {
        inFlight.remove(part)
    }

    /** Current best estimate, clamped to totalBytes. Private so callers can't
     * bypass the monotonic high-water guarantee of [nextEmit]. */
    @Synchronized
    private fun current(): Long {
        val sum = completedBytes + inFlight.values.sum()
        return if (sum > totalBytes) totalBytes else sum
    }

    /**
     * The current byte count, but only when it advances past the high-water
     * mark; otherwise null. Callers emit an `upload-progress` event iff non-null.
     */
    @Synchronized
    fun nextEmit(): Long? {
        val now = current()
        if (now <= lastEmittedBytes) return null
        lastEmittedBytes = now
        return now
    }
}
