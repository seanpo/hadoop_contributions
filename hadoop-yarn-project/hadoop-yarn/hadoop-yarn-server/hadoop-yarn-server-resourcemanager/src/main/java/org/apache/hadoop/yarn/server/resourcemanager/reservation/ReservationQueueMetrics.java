/*******************************************************************************
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 ******************************************************************************/

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import static org.apache.hadoop.metrics2.lib.Interns.info;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Splitter;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;

/**
 * {@link ReservationQueueMetrics} is used to collect metrics for the
 * {@link ReservationSystem} and its constituent components. The
 */
@InterfaceAudience.Private
@Metrics(context = "yarn")
public final class ReservationQueueMetrics {

  static final Splitter Q_SPLITTER =
      Splitter.on('.').omitEmptyStrings().trimResults();
  private static MetricsRegistry registry;

  /**
   * Helper method to clear cache.
   */
  @InterfaceAudience.Private
  public synchronized static void clearReservationMetrics() {
    RESERVATION_METRICS.clear();
  }

  /**
   * Simple metrics cache to help prevent re-registrations.
   */
  private static final Map<String, ReservationQueueMetrics> RESERVATION_METRICS =
      new HashMap<>();

  /**
   * Gets the instance of {@link ReservationQueueMetrics} for a particular
   * queue. If an instance did not already exist, a new one will be created. The
   * {@link MetricsSystem} used to register these metrics is the singleton
   * {@link DefaultMetricsSystem#instance()}.
   */
  public synchronized static ReservationQueueMetrics forReservationQueue(
      String queueName, Queue parent) {
    return forReservationQueue(DefaultMetricsSystem.instance(), queueName,
        parent);
  }

  /**
   * Gets the instance of {@link ReservationQueueMetrics} for a particular
   * queue. If an instance did not already exist, a new one will be created.
   */
  public synchronized static ReservationQueueMetrics forReservationQueue(
      MetricsSystem ms, String queueName, Queue parent) {
    ReservationQueueMetrics metrics = RESERVATION_METRICS.get(queueName);
    if (metrics == null) {
      metrics = new ReservationQueueMetrics(queueName, parent);

      // Register with the MetricsSystems
      if (ms != null) {
        metrics = ms.register(sourceName(queueName).toString(),
            "Metrics for queue: " + queueName, metrics);
      }
      RESERVATION_METRICS.put(queueName, metrics);
    }

    return metrics;
  }

  private MutableQuantiles planAddReservationLatency;

  private MutableQuantiles planUpdateReservationLatency;

  private MutableQuantiles planDeleteReservationLatency;

  private MutableQuantiles planFollowerSynchronizeLatency;

  private MutableQuantiles reservationAgentCreateReservationLatency;

  private MutableQuantiles reservationAgentUpdateReservationLatency;

  private MutableQuantiles reservationAgentDeleteReservationLatency;

  @Metric("Plan Add Reservation Total Count")
  private MutableCounterInt planAddReservationTotalCount;

  @Metric("Plan Update Reservation Total Count")
  private MutableCounterInt planUpdateReservationTotalCount;

  @Metric("Plan Delete Reservation Total Count")
  private MutableCounterInt planDeleteReservationTotalCount;

  @Metric("Plan Add Reservation Failure Count")
  private MutableCounterInt planAddReservationFailureCount;

  @Metric("Plan Update Reservation Failure Count")
  private MutableCounterInt planUpdateReservationFailureCount;

  @Metric("Plan Delete Reservation Failure Count")
  private MutableCounterInt planDeleteReservationFailureCount;

  @Metric("Plan Follower Synchronize Count")
  private MutableCounterInt planFollowerSynchronizeCount;

  private String queueName;

  private Queue parent;

  private static final MetricsInfo RECORD_INFO =
      info("ReservationQueueMetrics", "Reservation Metrics by Queue");

  private ReservationQueueMetrics(String queueName, Queue parent) {
    this.queueName = queueName;
    this.parent = parent;

    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "ReservationMetricsFor" + queueName);
  }

  private void initialize() {
    planAddReservationLatency =
        registry.newQuantiles("ReservationAgentAddReservationLatency",
            "Latency for create reservation", "ops", "latency", 5);

    planUpdateReservationLatency =
        registry.newQuantiles("ReservationAgentUpdateReservationLatency",
            "Latency for update reservation", "ops", "latency", 5);

    planDeleteReservationLatency =
        registry.newQuantiles("ReservationAgentDeleteReservationLatency",
            "Latency for remove reservation", "ops", "latency", 5);

    planFollowerSynchronizeLatency =
        registry.newQuantiles("PlanFollowerSynchronizeLatency",
            "Latency for plan follower execution", "ops", "latency", 5);

    reservationAgentCreateReservationLatency =
        registry.newQuantiles("ReservationAgentCreateReservationLatency",
            "Latency for create reservation", "ops", "latency", 5);

    reservationAgentUpdateReservationLatency =
        registry.newQuantiles("ReservationAgentUpdateReservationLatency",
            "Latency for update reservation", "ops", "latency", 5);

    reservationAgentDeleteReservationLatency =
        registry.newQuantiles("ReservationAgentDeleteReservationLatency",
            "Latency for delete reservation", "ops", "latency", 5);
  }

  public void setPlanAddReservationMetrics(long latency, boolean success) {
    planAddReservationLatency.add(latency);
    planAddReservationTotalCount.incr();
    if (!success) {
      planAddReservationFailureCount.incr();
    }
  }

  public void setPlanUpdateReservationMetrics(long latency, boolean success) {
    planUpdateReservationLatency.add(latency);
    planUpdateReservationTotalCount.incr();
    if (!success) {
      planUpdateReservationFailureCount.incr();
    }
  }

  public void setPlanDeleteReservationMetrics(long latency, boolean success) {
    planDeleteReservationLatency.add(latency);
    planDeleteReservationTotalCount.incr();
    if (!success) {
      planDeleteReservationFailureCount.incr();
    }
  }

  public void setPlanFollowerSynchronizeMetrics(long latency) {
    planFollowerSynchronizeLatency.add(latency);
    planFollowerSynchronizeCount.incr();
  }

  public void setAgentCreateReservationMetrics(long latency) {
    reservationAgentCreateReservationLatency.add(latency);
  }

  public void setAgentUpdateReservationMetrics(long latency) {
    reservationAgentUpdateReservationLatency.add(latency);
  }

  public void setAgentDeleteReservationMetrics(long latency) {
    reservationAgentDeleteReservationLatency.add(latency);
  }

  protected static StringBuilder sourceName(String queueName) {
    StringBuilder sb = new StringBuilder(RECORD_INFO.name());
    int i = 0;
    for (String node : Q_SPLITTER.split(queueName)) {
      sb.append(",q").append(i++).append('=').append(node);
    }
    return sb;
  }
}
