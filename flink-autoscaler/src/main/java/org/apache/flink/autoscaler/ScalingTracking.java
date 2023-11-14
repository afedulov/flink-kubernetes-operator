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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
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

/** Stores rescaling related information. */
@Experimental
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ScalingTracking {

    /** Time of starting the last rescaling. */
    @JsonIgnore private long lastRescalingStartTimestamp;

    /** Details related to recent rescaling operations. */
    private final TreeMap<Instant, ScalingRecord> scalingRecords = new TreeMap<>();

    // TODO: extract into a config parameter (currently not used)
    @JsonIgnore private final Duration keptTimeSpan = Duration.ofHours(1);

    public void addScalingRecord(Instant startTimestamp, ScalingRecord scalingRecord) {
        scalingRecords.put(startTimestamp, scalingRecord);
    }

    @JsonIgnore
    public Optional<Entry<Instant, ScalingRecord>> getLastScalingRecordEntry() {
        if (!scalingRecords.isEmpty()) {
            return Optional.of(scalingRecords.lastEntry());
        } else {
            return Optional.empty();
        }
    }

    public boolean setEndTimeIfTrackedAndParallelismMatches(
            Instant now,
            JobTopology jobTopology,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory) {
        return getLastScalingRecordEntry()
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
                                    value.setEndTime(now);
                                    return true;
                                }
                            }
                            return false;
                        })
                .orElse(false);
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

    // TODO: currently not used, discuss cleanup
    private void removeOldEvents() {
        var threshold = Instant.now().minus(keptTimeSpan);
        var entry = scalingRecords.pollFirstEntry();
        while (entry != null && entry.getKey().compareTo(threshold) <= 0) {
            scalingRecords.remove(entry.getKey());
            entry = scalingRecords.pollFirstEntry();
        }
    }

    public double getMaxRestartTimeOrDefault(Configuration conf) {
        long maxRestartTime = -1;
        long restartTimeFromConfig = conf.get(AutoScalerOptions.RESTART_TIME).toSeconds();
        if (conf.get(AutoScalerOptions.PREFER_TRACKED_RESTART_TIME)) {
            int maxNumOfSamples = conf.get(AutoScalerOptions.NUM_RESTART_SAMPLES);
            int numSamples = 0;
            for (Map.Entry<Instant, ScalingRecord> entry : scalingRecords.entrySet()) {
                if (numSamples == maxNumOfSamples) {
                    break;
                }
                var startTime = entry.getKey();
                var endTime = entry.getValue().getEndTime();
                if (endTime != null) {
                    var restartTime = Duration.between(startTime, endTime).toSeconds();
                    maxRestartTime = Math.max(restartTime, maxRestartTime);
                    numSamples++;
                }
            }
        }
        return maxRestartTime == -1
                ? restartTimeFromConfig
                : Math.min(maxRestartTime, restartTimeFromConfig);
    }
}
