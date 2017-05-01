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
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;

@InterfaceAudience.Private
@Metrics(context = "yarn")
public class PriorityReservationAgentMetrics {

  private static AtomicBoolean isInitialized = new AtomicBoolean(false);
  private static PriorityReservationAgentMetrics instance = null;
  private static MetricsRegistry registry;

  @Metric("No Priority Inversion Create Reservation Latency")
  private MutableQuantiles createReservationWithoutPriorityInversionLatency;

  @Metric("No Priority Inversion Update Reservation Latency")
  private MutableQuantiles updateReservationWithoutPriorityInversionLatency;

  @Metric("Priority Inversion Create Reservation Latency")
  private MutableQuantiles createReservationWithPriorityInversionLatency;

  @Metric("Priority Inversion Update Reservation Latency")
  private MutableQuantiles updateReservationWithPriorityInversionLatency;

  @Metric("Delete Reservation Latency")
  private MutableQuantiles deleteReservationLatency;

  @Metric("PriorityReservationAgent Priority Inversion Rate")
  private MutableRate priorityInversionRate;

  private static final MetricsInfo RECORD_INFO =
      info("PriorityReservationAgentMetrics", "PriorityReservationAgent");

  public static PriorityReservationAgentMetrics getMetrics() {
    if (!isInitialized.get()) {
      synchronized (PriorityReservationAgentMetrics.class) {
        if (instance == null) {
          instance = new PriorityReservationAgentMetrics();
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
      ms.register("PriorityReservationAgentMetrics",
          "Metrics for the Yarn PriorityReservationAgent", instance);
    }
  }

  private void initialize() {
    createReservationWithoutPriorityInversionLatency = registry.newQuantiles(
        "CreateReservationWithoutPriorityInversionLatency", "inversion", "Ops",
        "Latency", 5);
    updateReservationWithoutPriorityInversionLatency = registry.newQuantiles(
        "UpdateReservationWithoutPriorityInversionLatency", "inversion", "Ops",
        "Latency", 5);
    createReservationWithPriorityInversionLatency =
        registry.newQuantiles("CreateReservationWithPriorityInversionLatency",
            "Latency when a reservation is created with priority inversion",
            "Ops", "Latency", 5);
    updateReservationWithPriorityInversionLatency =
        registry.newQuantiles("UpdateReservationWithPriorityInversionLatency",
            "Latency when a reservation is updated with priority inversion",
            "Ops", "Latency", 5);
    deleteReservationLatency = registry.newQuantiles("DeleteReservationLatency",
        "Latency when a reservation is deleted", "Ops", "Latency", 5);
    priorityInversionRate = registry.newRate("PriorityInversionRate");
  }

  public void setCreateReservationLatency(long latency,
      boolean priorityInversion) {
    if (priorityInversion) {
      createReservationWithPriorityInversionLatency.add(latency);
    } else {
      createReservationWithoutPriorityInversionLatency.add(latency);
    }
  }

  public void setUpdateReservationLatency(long latency,
      boolean priorityInversion) {
    if (priorityInversion) {
      updateReservationWithPriorityInversionLatency.add(latency);
    } else {
      updateReservationWithoutPriorityInversionLatency.add(latency);
    }
  }

  public void setDeleteReservationLatency(long latency) {
    deleteReservationLatency.add(latency);
  }

  public void incrementPriorityInversion(long entries) {
    priorityInversionRate.add(entries);
  }
}
