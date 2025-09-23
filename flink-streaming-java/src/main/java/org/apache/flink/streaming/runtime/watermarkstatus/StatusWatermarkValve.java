/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.watermarkstatus;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A {@code StatusWatermarkValve} embodies the logic of how {@link Watermark} and {@link
 * WatermarkStatus} are propagated to downstream outputs, given a set of one or multiple input
 * channels that continuously receive them. Usages of this class need to define the number of input
 * channels that the valve needs to handle, as well as provide a implementation of {@link
 * DataOutput}, which is called by the valve only when it determines a new watermark or watermark
 * status can be propagated.
 */
@Internal
public class StatusWatermarkValve {

    // ------------------------------------------------------------------------
    //	Runtime state for watermark & watermark status output determination
    // ------------------------------------------------------------------------

    /**
     * Array of current status of all input channels. Changes as watermarks & watermark statuses are
     * fed into the valve.
     */
    private final InputChannelStatus[] channelStatuses;

    /** The last watermark emitted from the valve. */
    private long lastOutputWatermark;

    /** The last watermark status emitted from the valve. */
    private WatermarkStatus lastOutputWatermarkStatus;

    /**
     * Returns a new {@code StatusWatermarkValve}.
     *
     * @param numInputChannels the number of input channels that this valve will need to handle
     */
    public StatusWatermarkValve(int numInputChannels) {
        checkArgument(numInputChannels > 0);
        this.channelStatuses = new InputChannelStatus[numInputChannels];
        for (int i = 0; i < numInputChannels; i++) {
            channelStatuses[i] = new InputChannelStatus();
            channelStatuses[i].watermark = Long.MIN_VALUE;
            channelStatuses[i].watermarkStatus = WatermarkStatus.ACTIVE;
            channelStatuses[i].isWatermarkAligned = true;
        }

        this.lastOutputWatermark = Long.MIN_VALUE;
        this.lastOutputWatermarkStatus = WatermarkStatus.ACTIVE;
    }

    /**
     * Feed a {@link Watermark} into the valve. If the input triggers the valve to output a new
     * Watermark, {@link DataOutput#emitWatermark(Watermark)} will be called to process the new
     * Watermark.
     *
     * @param watermark the watermark to feed to the valve
     * @param channelIndex the index of the channel that the fed watermark belongs to (index
     *     starting from 0)
     */
    public void inputWatermark(Watermark watermark, int channelIndex, DataOutput<?> output)
            throws Exception {
        // ignore the input watermark if its input channel, or all input channels are idle (i.e.
        // overall the valve is idle).
        if (lastOutputWatermarkStatus.isActive()
                && channelStatuses[channelIndex].watermarkStatus.isActive()) {
            long watermarkMillis = watermark.getTimestamp();

            // if the input watermark's value is less than the last received watermark for its input
            // channel, ignore it also.
            if (watermarkMillis > channelStatuses[channelIndex].watermark) {
                channelStatuses[channelIndex].watermark = watermarkMillis;

                // previously unaligned input channels are now aligned if its watermark has caught
                // up
                if (!channelStatuses[channelIndex].isWatermarkAligned
                        && watermarkMillis >= lastOutputWatermark) {
                    channelStatuses[channelIndex].isWatermarkAligned = true;
                }

                // now, attempt to find a new min watermark across all aligned channels
                Long newWatermark = calculateMinWatermarkFromAlignedChannels();
                if (newWatermark != null && newWatermark > lastOutputWatermark) {
                    lastOutputWatermark = newWatermark;
                    output.emitWatermark(new Watermark(lastOutputWatermark));
                }
            }
        }
    }

    /**
     * Feed a {@link WatermarkStatus} into the valve. This may trigger the valve to output either a
     * new Watermark Status, for which {@link DataOutput#emitWatermarkStatus(WatermarkStatus)} will
     * be called, or a new Watermark, for which {@link DataOutput#emitWatermark(Watermark)} will be
     * called.
     *
     * @param watermarkStatus the watermark status to feed to the valve
     * @param channelIndex the index of the channel that the fed watermark status belongs to (index
     *     starting from 0)
     */
    public void inputWatermarkStatus(
            WatermarkStatus watermarkStatus, int channelIndex, DataOutput<?> output)
            throws Exception {

        WatermarkStatus currentStatus = channelStatuses[channelIndex].watermarkStatus;

        // Early exit if no status change
        if (watermarkStatus.equals(currentStatus)) {
            return;
        }

        // Update channel status and alignment
        updateChannelStatusAndAlignment(watermarkStatus, channelIndex, currentStatus);

        // Recalculate and emit watermark/status if needed (preserving optimization)
        recalculateAndEmitWatermarkAndStatus(output, channelIndex);
    }

    // Updates channel status and watermark alignment based on status transition.
    private void updateChannelStatusAndAlignment(
            WatermarkStatus newStatus, int channelIndex, WatermarkStatus currentStatus) {

        channelStatuses[channelIndex].watermarkStatus = newStatus;

        if (newStatus.isFinished()) {
            // FINISHED channels are excluded from watermark aggregation
            channelStatuses[channelIndex].isWatermarkAligned = false;
        } else if (newStatus.isIdle()) {
            // IDLE channels are not aligned
            channelStatuses[channelIndex].isWatermarkAligned = false;
        } else if (newStatus.isActive() && currentStatus.isIdle()) {
            // Reactivating from IDLE - check if watermark has caught up
            if (channelStatuses[channelIndex].watermark >= lastOutputWatermark) {
                channelStatuses[channelIndex].isWatermarkAligned = true;
            }
            // Otherwise remains unaligned until watermark catches up via inputWatermark()
        }
    }

    /**
     * Recalculates watermark and status based on aggregation rules, with optimization to only
     * recalculate when the changed channel was contributing to current watermark.
     * Uses scenario-specific emission order to preserve original semantics:
     * - FINISHED transitions: Status first (termination signal) → Watermark (tombstone)
     * - IDLE transitions: Watermark first (final progression) → Status (state change)
     * - ACTIVE transitions: Status only (reactivation signal)
     */
    private void recalculateAndEmitWatermarkAndStatus(DataOutput<?> output, int channelIndex)
            throws Exception {

        WatermarkStatus newOverallStatus = determineOverallStatus();
        boolean shouldRecalculateWatermark =
                channelStatuses[channelIndex].watermark == lastOutputWatermark;
        boolean statusChanged = !newOverallStatus.equals(lastOutputWatermarkStatus);

        if (!statusChanged && !shouldRecalculateWatermark) {
            return; // No changes needed
        }

        // FINISHED transition: Status first (termination) → Watermark (tombstone)
        if (statusChanged && newOverallStatus.isFinished()) {
            lastOutputWatermarkStatus = WatermarkStatus.FINISHED;
            output.emitWatermarkStatus(lastOutputWatermarkStatus);

            // Then emit MAX_VALUE as tombstone
            lastOutputWatermark = Long.MAX_VALUE;
            output.emitWatermark(new Watermark(lastOutputWatermark));

            // IDLE transition: Watermark first (final progression) → Status (state change)
        } else if (statusChanged && newOverallStatus.isIdle()) {
            // Emit final watermark progression before going idle
            if (shouldRecalculateWatermark) {
                tryEmitNewWatermark(output);
            }

            // Then signal idle state
            lastOutputWatermarkStatus = WatermarkStatus.IDLE;
            output.emitWatermarkStatus(lastOutputWatermarkStatus);

            // ACTIVE transition: Status only (reactivation signal)
        } else if (statusChanged && newOverallStatus.isActive()) {
            lastOutputWatermarkStatus = WatermarkStatus.ACTIVE;
            output.emitWatermarkStatus(lastOutputWatermarkStatus);
            // Note: Watermarks will come through normal inputWatermark() flow

            // Only watermark changed, no status change
        } else if (shouldRecalculateWatermark) {
            tryEmitNewWatermark(output);
        }
    }

    // Helper to calculate and emit new watermark if it progresses.
    private void tryEmitNewWatermark(DataOutput<?> output) throws Exception {
        Long newWatermark = calculateWatermarkByAggregationRules();
        if (newWatermark != null && newWatermark > lastOutputWatermark) {
            lastOutputWatermark = newWatermark;
            output.emitWatermark(new Watermark(lastOutputWatermark));
        }
    }

    /**
     * Calculates watermark based on the clear aggregation rules:
     * 1. If there are ACTIVE channels: watermark = min(active_channels)
     * 2. Else if there are IDLE channels: watermark = max(idle_channels)
     * 3. Else (all channels FINISHED): watermark = Long.MAX_VALUE
     */
    private Long calculateWatermarkByAggregationRules() {
        if (InputChannelStatus.hasActiveChannels(channelStatuses)) {
            // Rule 1: ACTIVE channels exist -> min(active_channels)
            return calculateMinWatermarkFromAlignedChannels();
        } else if (hasIdleChannels()) {
            // Rule 2: Only IDLE channels (no active) -> max(idle_channels)
            return calculateMaxWatermarkFromNonFinishedChannels();
        } else {
            // Rule 3: All channels FINISHED -> Long.MAX_VALUE
            return Long.MAX_VALUE;
        }
    }

    // Calculates minimum watermark from aligned (active) channels.
    private Long calculateMinWatermarkFromAlignedChannels() {
        long minWatermark = Long.MAX_VALUE;
        boolean hasAlignedChannels = false;

        for (InputChannelStatus channelStatus : channelStatuses) {
            if (channelStatus.isWatermarkAligned) {
                hasAlignedChannels = true;
                minWatermark = Math.min(channelStatus.watermark, minWatermark);
            }
        }

        return hasAlignedChannels ? minWatermark : null;
    }

    // Calculates maximum watermark from non-finished (idle) channels.
    private Long calculateMaxWatermarkFromNonFinishedChannels() {
        long maxWatermark = Long.MIN_VALUE;
        boolean hasNonFinishedChannels = false;

        for (InputChannelStatus channelStatus : channelStatuses) {
            if (!channelStatus.watermarkStatus.isFinished()) {
                hasNonFinishedChannels = true;
                maxWatermark = Math.max(channelStatus.watermark, maxWatermark);
            }
        }

        return hasNonFinishedChannels ? maxWatermark : null;
    }

    // Checks if there are any IDLE channels
    private boolean hasIdleChannels() {
        for (InputChannelStatus channelStatus : channelStatuses) {
            if (channelStatus.watermarkStatus.isIdle()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Determines the overall status:
     * 1. If there are ACTIVE channels: status = ACTIVE
     * 2. Else if there are IDLE channels: status = IDLE
     * 3. Else (all channels FINISHED): status = FINISHED
     */
    private WatermarkStatus determineOverallStatus() {
        boolean hasActive = false;
        boolean hasIdle = false;

        for (InputChannelStatus status : channelStatuses) {
            if (status.watermarkStatus.isActive()) {
                hasActive = true;
            } else if (status.watermarkStatus.isIdle()) {
                hasIdle = true;
            }
        }

        if (hasActive) {
            return WatermarkStatus.ACTIVE;
        } else if (hasIdle) {
            return WatermarkStatus.IDLE;
        } else {
            return WatermarkStatus.FINISHED;
        }
    }

    /**
     * An {@code InputChannelStatus} keeps track of an input channel's last watermark, stream
     * status, and whether or not the channel's current watermark is aligned with the overall
     * watermark output from the valve.
     *
     * <p>There are 2 situations where a channel's watermark is not considered aligned:
     *
     * <ul>
     *   <li>the current watermark status of the channel is idle
     *   <li>the watermark status has resumed to be active, but the watermark of the channel hasn't
     *       caught up to the last output watermark from the valve yet.
     * </ul>
     */
    @VisibleForTesting
    protected static class InputChannelStatus {
        protected long watermark;
        protected WatermarkStatus watermarkStatus;
        protected boolean isWatermarkAligned;

        /**
         * Utility to check if at least one channel in a given array of input channels is active.
         */
        private static boolean hasActiveChannels(InputChannelStatus[] channelStatuses) {
            for (InputChannelStatus status : channelStatuses) {
                if (status.watermarkStatus.isActive()) {
                    return true;
                }
            }
            return false;
        }
    }

    @VisibleForTesting
    protected InputChannelStatus getInputChannelStatus(int channelIndex) {
        Preconditions.checkArgument(
                channelIndex >= 0 && channelIndex < channelStatuses.length,
                "Invalid channel index. Number of input channels: " + channelStatuses.length);

        return channelStatuses[channelIndex];
    }
}
