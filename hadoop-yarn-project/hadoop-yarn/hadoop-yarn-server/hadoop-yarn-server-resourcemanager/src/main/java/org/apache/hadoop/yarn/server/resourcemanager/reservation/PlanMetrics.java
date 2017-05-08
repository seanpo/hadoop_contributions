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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;

/**
 * {@link PlanMetrics} is used to collect metrics for the
 * {@link Plan}. Specifically, the latency, total count, and failure count of
 * the addReservation, updateReservation, and deleteReservation methods are
 * collected.
 */
@InterfaceAudience.Private
@Metrics(context = "yarn")
public final class PlanMetrics {

  private static AtomicBoolean isInitialized = new AtomicBoolean(false);
  private static PlanMetrics instance = null;
  private static MetricsRegistry registry;

  private MutableQuantiles planAddReservationLatency;

  private MutableQuantiles planUpdateReservationLatency;

  private MutableQuantiles planDeleteReservationLatency;

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

  private static final MetricsInfo RECORD_INFO =
      info("PlanMetrics", "Plan Metrics for Yarn");

  private PlanMetrics() {
  }

  public static PlanMetrics getMetrics() {
    if (!isInitialized.get()) {
      synchronized (PlanMetrics.class) {
        if (instance == null) {
          instance = new PlanMetrics();
          registerMetrics();
          instance.initialize();
          isInitialized.set(true);
        }
      }
    }
    return instance;
  }

  private static void registerMetrics() {
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "ResourceManager");
    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms != null) {
      ms.register("PlanMetrics", "Plan Metrics for Yarn",
          instance);
    }
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
}
