/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.autoscaler;

import lombok.AllArgsConstructor;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Stores rescaling related information for the job. */
@Experimental
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ScalingTracking {

    /** Details related to recent rescaling operations. */
    private final TreeMap<Instant, ScalingRecord> scalingRecords = new TreeMap<>();

    /** Exponential moving average of restart time */
    private Duration restartTimeEMA;

    private double emaSmoothingFactor = 0.5d;

    public void addScalingRecord(Instant startTimestamp, ScalingRecord scalingRecord) {
        scalingRecords.put(startTimestamp, scalingRecord);
    }

    @JsonIgnore
    public Optional<Entry<Instant, ScalingRecord>> getLatestScalingRecordEntry() {
        if (!scalingRecords.isEmpty()) {
            return Optional.of(scalingRecords.lastEntry());
        } else {
            return Optional.empty();
        }
    }

    /**
     * Sets the end time for the latest scaling record if its parallelism matches the current job
     * parallelism. Additionally, it updates the exponential moving average that tracks restart
     * time, if this condition is met.
     *
     * @param now The current instant to be set as the end time of the scaling record.
     * @param jobTopology The current job topology containing details of the job's parallelism.
     * @param scalingHistory The scaling history.
     * @return true if the end time is successfully set, false if the end time is already set, the
     *     latest scaling record cannot be found, or the target parallelism does not match the
     *     actual parallelism.
     */
    public boolean setEndTimeIfParallelismMatches(
            Instant now,
            JobTopology jobTopology,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory,
            Duration restartTimeFromConfig) {
        return getLatestScalingRecordEntry()
                .map(
                        entry -> {
                            var value = entry.getValue();
                            var scalingTimestamp = entry.getKey();
                            if (value.getEndTime() == null) {
                                var targetParallelism =
                                        getTargetParallelismOfScaledVertices(
                                                scalingTimestamp, scalingHistory);
                                var actualParallelism = jobTopology.getParallelisms();

                                if (targetParallelismMatchesActual(
                                        targetParallelism, actualParallelism)) {
                                    var restartTime = Duration.between(scalingTimestamp, now);
                                    restartTimeEMA =
                                            calculateEMA(restartTime, restartTimeFromConfig);
                                    value.setEndTime(now);
                                    return true;
                                }
                            }
                            return false;
                        })
                .orElse(false);
    }

    private Duration calculateEMA(Duration restartTime, Duration defaultRestartTime) {
        if (restartTime == null) {
            return defaultRestartTime;
        }
        return Duration.ofSeconds(
                (long)
                        (restartTimeEMA.getSeconds() * emaSmoothingFactor
                                + restartTime.getSeconds() * (1 - emaSmoothingFactor)));
    }

    private static Map<JobVertexID, Integer> getTargetParallelismOfScaledVertices(
            Instant scalingTimestamp,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory) {
        return scalingHistory.entrySet().stream()
                .filter(entry -> entry.getValue().containsKey(scalingTimestamp))
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry ->
                                        entry.getValue()
                                                .get(scalingTimestamp)
                                                .getNewParallelism()));
    }

    private static boolean targetParallelismMatchesActual(
            Map<JobVertexID, Integer> targetParallelisms,
            Map<JobVertexID, Integer> actualParallelisms) {
        return targetParallelisms.entrySet().stream()
                .allMatch(
                        entry -> {
                            var vertexID = entry.getKey();
                            var targetParallelism = entry.getValue();
                            var actualParallelism = actualParallelisms.getOrDefault(vertexID, -1);
                            return actualParallelism.equals(targetParallelism);
                        });
    }

    /**
     * Retrieves the maximum restart time in seconds based on the provided configuration and scaling
     * records. Defaults to the RESTART_TIME from configuration if the PREFER_TRACKED_RESTART_TIME
     * option is set to false, or if there are no tracking records available. Otherwise, the maximum
     * observed restart time is capped by the MAX_RESTART_TIME.
     */
    public Duration getMaxRestartTimeSecondsOrDefault(Configuration conf) {
        long maxRestartTime = -1;
        if (conf.get(AutoScalerOptions.PREFER_TRACKED_RESTART_TIME)) {
            for (Map.Entry<Instant, ScalingRecord> entry : scalingRecords.entrySet()) {
                var startTime = entry.getKey();
                var endTime = entry.getValue().getEndTime();
                if (endTime != null) {
                    var restartTime = Duration.between(startTime, endTime).toSeconds();
                    maxRestartTime = Math.max(restartTime, maxRestartTime);
                }
            }
        }
        var restartTimeFromConfig = conf.get(AutoScalerOptions.RESTART_TIME);
        long maxRestartTimeFromConfig =
                conf.get(AutoScalerOptions.TRACKED_RESTART_TIME_LIMIT).toSeconds();
        return maxRestartTime == -1
                ? restartTimeFromConfig
                : Duration.ofSeconds(Math.min(maxRestartTime, maxRestartTimeFromConfig));
    }

    /**
     * Retrieves the tracked restart time exponential moving average (EMA) based on the provided
     * configuration. Defaults to the RESTART_TIME from configuration if the
     * PREFER_TRACKED_RESTART_TIME option is set to false, or if no restarts where yet observed.
     * Otherwise, the returned restart time EMA is capped by the TRACKED_RESTART_TIME_LIMIT.
     */
    public Duration getRestartTimeEMAOrDefault(Configuration conf) {
        long maxRestartTimeFromConfig =
                conf.get(AutoScalerOptions.TRACKED_RESTART_TIME_LIMIT).toSeconds();
        if (conf.get(AutoScalerOptions.PREFER_TRACKED_RESTART_TIME)) {
            if (restartTimeEMA != null) {
                return Duration.ofSeconds(
                        Math.min(restartTimeEMA.toSeconds(), maxRestartTimeFromConfig));
            }
        }
        return conf.get(AutoScalerOptions.RESTART_TIME);
    }

    /**
     * Removes records from the internal map that are older than the specified time span and trims
     * the number of records to the specified maximum count.
     *
     * @param keptTimeSpan Duration for how long recent records are to be kept.
     * @param keptNumRecords The maximum number of recent records to keep.
     */
    public void removeOldRecords(Instant now, Duration keptTimeSpan, int keptNumRecords) {
        var cutoffTime = now.minus(keptTimeSpan);

        // Remove records older than the cutoff time
        scalingRecords.headMap(cutoffTime).clear();

        // If the map size is still larger than keptNumRecords, trim further
        while (scalingRecords.size() > keptNumRecords) {
            scalingRecords.pollFirstEntry();
        }
    }
}
