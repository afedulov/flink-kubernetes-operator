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

package org.apache.flink.kubernetes.operator.api.status;

import org.apache.flink.annotation.Experimental;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Stores rescaling related information. */
@Experimental
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RescalingStats {

    /** Time of starting the last rescaling. */
    private long lastRescalingStartTimestamp;

    /** Details related to recent rescaling operations. */
    private final TreeMap<Long, Rescaling> rescalings = new TreeMap<>();

    // TODO: extract into a config parameter
    private final long oneHourInMillis = 60 * 60 * 1000;

    /** This flag is used to prevent adding new */
    private boolean lastRescalingTracked = false;

    public void addRescaling(Rescaling rescaling) {
        rescalings.put(rescaling.getStartTimestamp(), rescaling);
        lastRescalingTracked = true;
        removeOldEvents();
    }

    public List<Rescaling> getRescalings() {
        removeOldEvents();
        return new ArrayList<>(rescalings.values());
    }

    private void removeOldEvents() {
        long threshold = System.currentTimeMillis() - oneHourInMillis;
        Map.Entry<Long, Rescaling> entry = rescalings.pollFirstEntry();
        while (entry != null && entry.getKey() <= threshold) {
            rescalings.remove(entry.getKey());
            entry = rescalings.pollFirstEntry();
        }
    }
}
