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
 * {@link PlanFollowerMetrics} is used to collect and contain metrics for the
 * {@link PlanFollower}. Specifically, the metrics for synchronize method are
 * collected.
 */
@InterfaceAudience.Private
@Metrics(context = "yarn")
public final class PlanFollowerMetrics {

  private static AtomicBoolean isInitialized = new AtomicBoolean(false);
  private static PlanFollowerMetrics instance = null;
  private static MetricsRegistry registry;

  private MutableQuantiles planFollowerSynchronizeLatency;

  @Metric("Plan Follower Synchronize Count")
  private MutableCounterInt planFollowerSynchronizeCount;

  private static final MetricsInfo RECORD_INFO =
      info("PlanFollowerMetrics", "Metrics for the Yarn PlanFollower");

  private PlanFollowerMetrics() {
  }

  public static PlanFollowerMetrics getMetrics() {
    if (!isInitialized.get()) {
      synchronized (PlanFollowerMetrics.class) {
        if (instance == null) {
          instance = new PlanFollowerMetrics();
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
      ms.register("PlanFollowerMetrics", "Metrics for the Yarn PlanFollower",
          instance);
    }
  }

  private void initialize() {
    planFollowerSynchronizeLatency =
        registry.newQuantiles("PlanFollowerSynchronizeLatency",
            "Latency for plan follower execution", "ops", "latency", 5);
  }

  public void setPlanFollowerSynchronizeMetrics(long latency) {
    planFollowerSynchronizeLatency.add(latency);
    planFollowerSynchronizeCount.incr();
  }
}
