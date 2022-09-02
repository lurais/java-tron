package org.tron.common.prometheus;

import io.prometheus.client.Histogram;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j(topic = "metrics")
public class MetricsHistogram {

  private static final Map<String, Histogram> container = new ConcurrentHashMap<>();

  private static final Map<String, double[]> histogramBuckets = new HashMap<>();
  private static final double[] defaultBuckets =  new double[] { .005, .01, .025, .05, .075, .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10 };
  private static final double[] dbLatencyBuckets = new double[]{0.000001, 0.000005, 0.00001, 0.000109, 0.000159, 0.000208, 0.000258, 0.000307, 0.000357, 0.000406, 0.000456, 0.000505, 0.000554, 0.000604, 0.000653, 0.000703, 0.000752, 0.000802, 0.000851, 0.000901, 0.00095, 0.001, 0.002, 0.003, 0.004, 0.00502, 0.00522, 0.0053, 0.00537, 0.00543, 0.0055, 0.00557, 0.00564, 0.00574, 0.00587, 0.00606, 0.0064, 0.00694, 0.008, 0.016, 0.024, 0.032, 0.04, 0.048, 0.056, 0.064, 0.072, 0.08, 0.088, 0.096, 0.104, 0.112, 0.12, 0.128, 0.136, 0.144, 0.152, 0.16};
  private static final double[] checkPointBuckets = new double[]{0.01, 0.015, 0.022, 0.035, 0.037, 0.038, 0.04, 0.042, 0.044, 0.046, 0.05, 0.054, 0.06, 0.066, 0.072, 0.078, 0.087, 0.123, 0.297, 0.985, 1.327, 2.247, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0, 25.288, 28.369, 33.659, 34.78, 36.008, 37.591, 38.849, 39.965, 41.684, 44.2, 48.899, 54.137, 57.066, 59.282, 61.516, 63.437, 65.537, 68.832, 73.801, 86.965, 101.242};
  private static final double[] valueSizeBuckets = new double[]{1, 2, 4, 8, 16, 25, 33, 66, 99, 132, 165, 198, 231, 264, 297, 330, 363, 396, 429, 462, 495, 528, 561, 594, 627, 650, 660, 670, 680, 690, 700, 710, 720, 730, 740, 750, 760, 770, 780, 790, 800, 810, 820, 830, 840, 850, 980, 1110, 1240, 1370, 1500, 1630, 1760, 1890, 2020, 2150, 2280, 2410, 2540, 2670, 19000, 19700, 20400, 21100, 21800, 22500, 23200, 23900, 24600, 25300, 26000, 26700, 27400, 28100, 28800, 29500, 30200, 30900, 31600, 32300, 33000, 61000, 62000, 63000, 64000, 65000, 66000, 67000, 68000, 69000, 70000, 71000, 72000, 73000, 74000, 75000, 76000, 77000, 78000, 79000, 80000};

  static {
    initHistogramBuckets();
    init(MetricKeys.Histogram.INTERNAL_SERVICE_LATENCY, "Internal Service latency.",
        "class", "method");
    init(MetricKeys.Histogram.HTTP_SERVICE_LATENCY, "Http Service latency.",
        "url");
    init(MetricKeys.Histogram.GRPC_SERVICE_LATENCY, "Grpc Service latency.",
        "endpoint");
    init(MetricKeys.Histogram.MINER_LATENCY, "miner latency.",
        "miner");
    init(MetricKeys.Histogram.PING_PONG_LATENCY, "node  ping pong  latency.");
    init(MetricKeys.Histogram.VERIFY_SIGN_LATENCY, "verify sign latency for trx , block.",
        "type");
    init(MetricKeys.Histogram.LOCK_ACQUIRE_LATENCY, "lock acquire latency.",
        "type");
    init(MetricKeys.Histogram.BLOCK_PROCESS_LATENCY,
        "process block latency for TronNetDelegate.",
        "sync");
    init(MetricKeys.Histogram.BLOCK_PUSH_LATENCY, "push block latency for Manager.");
    init(MetricKeys.Histogram.BLOCK_GENERATE_LATENCY, "generate block latency.",
        "address");

    init(MetricKeys.Histogram.PROCESS_TRANSACTION_LATENCY, "process transaction latency.",
        "type", "contract");
    init(MetricKeys.Histogram.MINER_DELAY, "miner delay time, actualTime - planTime.",
        "miner");
    init(MetricKeys.Histogram.UDP_BYTES, "udp_bytes traffic.",
        "type");
    init(MetricKeys.Histogram.TCP_BYTES, "tcp_bytes traffic.",
        "type");
    init(MetricKeys.Histogram.HTTP_BYTES, "http_bytes traffic.",
        "url", "status");
    init(MetricKeys.Histogram.DB_SERVICE_LATENCY, "db service latency.",
            "type", "db", "op");
    init(MetricKeys.Histogram.CHECKPOINT_LATENCY, "checkpoint flush latency.",
            "type");
    init(MetricKeys.Histogram.DB_SERVICE_VALUE_BYTES, "db service value bytes.",
            "type", "db");
    init(MetricKeys.Histogram.SNAPSHOT_SERVICE_VALUE_BYTES, "snapshot value bytes.", "db");
    init(MetricKeys.Histogram.SNAPSHOT_SERVICE_LATENCY, "snapshot service latency", "db", "op");
  }

  private static void initHistogramBuckets() {
    histogramBuckets.put(MetricKeys.Histogram.DB_SERVICE_LATENCY, dbLatencyBuckets);
    histogramBuckets.put(MetricKeys.Histogram.CHECKPOINT_LATENCY, checkPointBuckets);
    histogramBuckets.put(MetricKeys.Histogram.DB_SERVICE_VALUE_BYTES, valueSizeBuckets);
    histogramBuckets.put(MetricKeys.Histogram.SNAPSHOT_SERVICE_VALUE_BYTES, valueSizeBuckets);
    histogramBuckets.put(MetricKeys.Histogram.SNAPSHOT_SERVICE_LATENCY, dbLatencyBuckets);
  }

  private MetricsHistogram() {
    throw new IllegalStateException("MetricsHistogram");
  }

  private static void init(String name, String help, String... labels) {
    container.put(name, Histogram.build().buckets(Objects.isNull(histogramBuckets.get(name))
                    ? defaultBuckets : histogramBuckets.get(name))
        .name(name)
        .help(help)
        .labelNames(labels)
        .register());
  }

  static Histogram.Timer startTimer(String key, String... labels) {
    if (Metrics.enabled()) {
      Histogram histogram = container.get(key);
      if (histogram == null) {
        logger.info("{} not exist", key);
        return null;
      }
      return histogram.labels(labels).startTimer();
    }
    return null;
  }

  static void observeDuration(Histogram.Timer startTimer) {
    if (startTimer != null) {
      startTimer.observeDuration();
    }
  }


  static void observe(String key, double amt, String... labels) {
    if (Metrics.enabled()) {
      Histogram histogram = container.get(key);
      if (histogram == null) {
        logger.info("{} not exist", key);
        return;
      }
      histogram.labels(labels).observe(amt);
    }
  }

}

