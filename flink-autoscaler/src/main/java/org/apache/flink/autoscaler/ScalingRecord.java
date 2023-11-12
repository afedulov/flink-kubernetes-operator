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

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.runtime.jobgraph.JobVertexID;

/**
 * Class for tracking scaling details, including time it took for the job to transition to the
 * target parallelism.
 */
@Data
@NoArgsConstructor
public class ScalingRecord {
    private Instant endTime;
    private Map<JobVertexID, Integer> targetParallelism;

    public ScalingRecord(Map<JobVertexID, Integer> targetParallelism) {
        this.targetParallelism = targetParallelism;
    }

    public static ScalingRecord from(Map<JobVertexID, ScalingSummary> scalingSummaries) {
        var targetParallelism =
                scalingSummaries.entrySet().stream()
                        .collect(
                                HashMap<JobVertexID, Integer>::new,
                                (map, entry) ->
                                        map.put(
                                                entry.getKey(),
                                                entry.getValue().getNewParallelism()),
                                Map::putAll);
        return new ScalingRecord(targetParallelism);
    }

    public boolean targetParallelismMatchesActual(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics) {
        return targetParallelism.entrySet().stream()
                .allMatch(
                        entry -> {
                            var vertexID = entry.getKey();
                            var target = entry.getValue();

                            System.out.println(">>> vertexID: " + vertexID);
                            System.out.println(">>> target: " + target);
                            var metricsMap = evaluatedMetrics.get(vertexID);

                            System.out.println(">>> metricsMap: " + metricsMap);
                            if (metricsMap == null) {
                                return false;
                            }

                            var actualWrapper = metricsMap.get(ScalingMetric.PARALLELISM);
                            System.out.println(">>> actualWrapper: " + actualWrapper);
                            if (actualWrapper == null) {
                                return false;
                            }
                            var actual = (int) actualWrapper.getCurrent();
                            System.out.println(">>> actual: " + actual);
                            return actual == target;
                        });
    }
}
