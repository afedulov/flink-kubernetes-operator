package org.apache.flink.autoscaler;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.time.Instant;

class ScalingTrackingTest {

    long configuredRestartTimeSeconds = 300;
    private ScalingTracking scalingTracking;
    private Configuration conf;

    @BeforeEach
    void setUp() {
        scalingTracking = new ScalingTracking();
        conf = new Configuration();
        conf.set(AutoScalerOptions.RESTART_TIME, Duration.ofSeconds(configuredRestartTimeSeconds));
        conf.set(AutoScalerOptions.PREFER_TRACKED_RESTART_TIME, true);
    }

    @Test
    void shouldReturnConfiguredRestartTime_WhenNoScalingRecords() {
        // Empty scalingTracking
        double result = scalingTracking.getMaxRestartTimeSecondsOrDefault(conf);

        assertThat(result).isEqualTo(configuredRestartTimeSeconds);
    }

    @Test
    void shouldReturnConfiguredRestartTime_WhenPreferTrackedRestartTimeIsFalse() {
        conf.set(AutoScalerOptions.PREFER_TRACKED_RESTART_TIME, false);
        setUpScalingRecords(Duration.ofSeconds(configuredRestartTimeSeconds - 1));

        double result = scalingTracking.getMaxRestartTimeSecondsOrDefault(conf);

        assertThat(result).isEqualTo(configuredRestartTimeSeconds);
    }

    @Test
    void shouldReturnMaxTrackedRestartTime_WhenNotCapped() {
        long duration = configuredRestartTimeSeconds - 1;
        setUpScalingRecords(Duration.ofSeconds(duration));

        double result = scalingTracking.getMaxRestartTimeSecondsOrDefault(conf);

        assertThat(result).isEqualTo(duration);
    }

    @Test
    void shouldReturnConfiguredRestartTime_WhenCapped() {
        long duration = configuredRestartTimeSeconds + 1;
        setUpScalingRecords(Duration.ofSeconds(duration));

        double result = scalingTracking.getMaxRestartTimeSecondsOrDefault(conf);

        assertThat(result).isEqualTo(configuredRestartTimeSeconds);
    }

    private void setUpScalingRecords(Duration secondRescaleDuration) {
        scalingTracking.addScalingRecord(
                Instant.parse("2023-11-15T16:00:00.00Z"),
                new ScalingRecord(Instant.parse("2023-11-15T16:03:00.00Z")));
        var secondRecordStart = Instant.parse("2023-11-15T16:20:00.00Z");
        scalingTracking.addScalingRecord(
                secondRecordStart,
                new ScalingRecord(secondRecordStart.plus(secondRescaleDuration)));
    }

    @Test
    void shouldSetEndTime_WhenParallelismMatches() {
        var now = Instant.now();
        var lastScaling = now.minusSeconds(60);
        addScalingRecordWithoutEndTime(lastScaling);
        var actualParallelisms = initActualParallelisms();
        var jobTopology = new JobTopology(createVertexInfoSet(actualParallelisms));
        var scalingHistory =
                initScalingHistoryWithTargetParallelism(lastScaling, actualParallelisms);

        boolean result =
                scalingTracking.setEndTimeIfTrackedAndParallelismMatches(
                        now, jobTopology, scalingHistory);

        assertThat(result).isTrue();
        assertThat(scalingTracking.getLatestScalingRecordEntry().get().getValue().getEndTime())
                .isEqualTo(now);
    }

    @Test
    void shouldNotSetEndTime_WhenParallelismDoesNotMatch() {
        var now = Instant.now();
        var lastScaling = now.minusSeconds(60);
        addScalingRecordWithoutEndTime(lastScaling);
        var actualParallelisms = initActualParallelisms();
        var jobTopology = new JobTopology(createVertexInfoSet(actualParallelisms));
        var mismatchedParallelisms = new HashMap<>(actualParallelisms);
        mismatchedParallelisms.replaceAll((key, value) -> value + 1);
        var scalingHistory =
                initScalingHistoryWithTargetParallelism(lastScaling, mismatchedParallelisms);

        boolean result =
                scalingTracking.setEndTimeIfTrackedAndParallelismMatches(
                        now, jobTopology, scalingHistory);

        assertThat(result).isFalse();
        assertThat(scalingTracking.getLatestScalingRecordEntry().get().getValue().getEndTime())
                .isNull();
    }

    private void addScalingRecordWithoutEndTime(Instant startTime) {
        ScalingRecord record = new ScalingRecord();
        scalingTracking.addScalingRecord(startTime, record);
    }

    private Set<VertexInfo> createVertexInfoSet(Map<JobVertexID, Integer> parallelisms) {
        Set<VertexInfo> vertexInfos = new HashSet<>();
        for (Map.Entry<JobVertexID, Integer> entry : parallelisms.entrySet()) {
            vertexInfos.add(
                    new VertexInfo(
                            entry.getKey(), new HashSet<>(), entry.getValue(), entry.getValue()));
        }
        return vertexInfos;
    }

    private Map<JobVertexID, Integer> initActualParallelisms() {
        var parallelisms = new HashMap<JobVertexID, Integer>();
        parallelisms.put(new JobVertexID(), 2);
        parallelisms.put(new JobVertexID(), 3);
        return parallelisms;
    }

    private Map<JobVertexID, SortedMap<Instant, ScalingSummary>>
            initScalingHistoryWithTargetParallelism(
                    Instant scalingTimestamp, Map<JobVertexID, Integer> targetParallelisms) {
        var history = new HashMap<JobVertexID, SortedMap<Instant, ScalingSummary>>();
        targetParallelisms.forEach(
                (id, parallelism) -> {
                    var vertexHistory = new TreeMap<Instant, ScalingSummary>();
                    vertexHistory.put(
                            scalingTimestamp,
                            new ScalingSummary(parallelism - 1, parallelism, null));
                    history.put(id, vertexHistory);
                });
        return history;
    }
}
