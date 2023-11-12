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

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.apache.flink.annotation.Experimental;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.TreeMap;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

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

    // TODO: extract into a config parameter
    @JsonIgnore private final Duration keptTimeSpan = Duration.ofHours(1);

    //    /** This flag is used to prevent adding new */
    //    private boolean lastRescalingTracked = false;

    public void addScalingRecord(Instant startTimestamp, ScalingRecord scalingRecord) {
        scalingRecords.put(startTimestamp, scalingRecord);
        //        lastRescalingTracked = true;
        //        removeOldEvents();
    }

    @JsonIgnore
    public Optional<ScalingRecord> getLastScalingRecord() {
        if (!scalingRecords.isEmpty()) {
            return Optional.of(scalingRecords.get(scalingRecords.lastKey()));
        } else {
            return Optional.empty();
        }
    }

    public TreeMap<Instant, ScalingRecord> getScalingRecords() {
        //        removeOldEvents();
        return scalingRecords;
    }

    public boolean setEndTimeIfTrackedAndParallelismMatches(
            Instant now,
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics) {
        return getLastScalingRecord()
                .map(
                        record -> {
                            if (record.getEndTime() == null
                                    && record.targetParallelismMatchesActual(evaluatedMetrics)) {
                                record.setEndTime(now);
                            }
                            return true;
                        })
                .orElse(false);
    }

    private void removeOldEvents() {
        var threshold = Instant.now().minus(keptTimeSpan);
        var entry = scalingRecords.pollFirstEntry();
        while (entry != null && entry.getKey().compareTo(threshold) <= 0) {
            scalingRecords.remove(entry.getKey());
            entry = scalingRecords.pollFirstEntry();
        }
    }

    public double getAverageRestartTimeOrDefault(Configuration conf) {
        if (conf.get(AutoScalerOptions.PREFER_TRACKED_RESTART_TIME)) {
            int numSamples = 0;
            int numRequiresSamples = conf.get(AutoScalerOptions.NUM_RESTART_SAMPLES);
            var acc = Duration.ZERO;
            for (Map.Entry<Instant, ScalingRecord> entry : scalingRecords.entrySet()) {
                if (numSamples == numRequiresSamples) {
                    break;
                }
                var startTime = entry.getKey();
                var endTime = entry.getValue().getEndTime();
                if (endTime == null) {
                    break; // only calculate if we have consecutive NUM_RESTART_SAMPLES
                }
                var restartTime = Duration.between(startTime, endTime);
                acc = acc.plus(restartTime);
                numSamples++;
            }
            if (numSamples == numRequiresSamples) {
                double averageRestartTime = (double) acc.toMillis() / numSamples;
            }
        }

        return conf.get(AutoScalerOptions.RESTART_TIME).toSeconds();
    }
}
