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

package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationPriorityScope;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * {@link SimplePriorityReservationAgent} employs the strategy of wrapping a
 * configured reservation agent to leverage the algorithm for reservation
 * submission, deletion, and update.
 *
 * If the reservation update, or submission fails,
 * {@link SimplePriorityReservationAgent} will delete all reservations in the
 * configured {@link ReservationPriorityScope} that are strictly lower priority
 * than the offending reservation before trying again. After the offending
 * reservation action has either succeeded or failed, the
 * {@link SimplePriorityReservationAgent} will attempt to re-add all the
 * reservations that have been deleted.
 */
public class SimplePriorityReservationAgent extends PriorityReservationAgent {

  private static final Logger LOG =
      LoggerFactory.getLogger(SimplePriorityReservationAgent.class);

  private ReservationPriorityScope scope;
  private Configuration configuration;

  public SimplePriorityReservationAgent() {
    this(new Configuration());
  }

  public SimplePriorityReservationAgent(Configuration conf) {
    setConf(conf);
  }

  public List<ReservationAllocation> makeRoomForReservation(
      ReservationId reservationId, String user, Plan plan,
      ReservationDefinition contract) {

    Set<ReservationAllocation> reservations;
    switch (scope) {
    case USER:
      // Get all reservations belonging to user;
      reservations = plan.getReservations(null, null, user);
      break;
    case QUEUE:
    default:
      // Get all reservations in the queue.
      reservations = plan.getAllReservations();
      break;
    }

    List<ReservationAllocation> yieldedReservations = new ArrayList<>();

    for (ReservationAllocation reservation : reservations) {
      if (contract.getPriority().getPriority() < reservation
          .getReservationDefinition().getPriority().getPriority()) {
        boolean success;

        // Try to delete the reservation. If fail, then do not add it to the
        // yielded list, or else it will be re-added as per the
        // PriorityReservationAgent contract. InMemoryPlan currently only
        // throws unchecked exceptions, which will only be thrown in the
        // event that the reservation with the provided id doesn't exist.
        try {
          success = plan.deleteReservation(reservation.getReservationId());
        } catch (Exception e) {
          LOG.error("Encountered exception when deleting reservation with "
              + "id=[" + reservation.getReservationId().toString() + "] for "
              + "yielding to a higher priority reservation with id=["
              + reservationId.toString() + "] due to exception=["
              + e.getMessage() + "].");
          success = false;
        }
        if (success) {
          yieldedReservations.add(reservation);
        }
      }
    }

    yieldedReservations.sort(new ReservationPriorityComparator());
    return yieldedReservations;
  }

  public Configuration getConf() {
    return configuration;
  }

  public void setConf(Configuration conf) {
    configuration = conf;
    reinitialize(conf);
  }

  private void reinitialize(Configuration conf) {
    scope =
        conf.getEnum(CapacitySchedulerConfiguration.RESERVATION_PRIORITY_SCOPE,
            CapacitySchedulerConfiguration.DEFAULT_RESERVATION_PRIORITY_SCOPE);
  }

  private static class ReservationPriorityComparator
      implements Comparator<ReservationAllocation>, Serializable {

    public int compare(ReservationAllocation reservationA,
        ReservationAllocation reservationB) {
      ReservationDefinition definitionA =
          reservationA == null ? null : reservationA.getReservationDefinition();
      ReservationDefinition definitionB =
          reservationB == null ? null : reservationB.getReservationDefinition();

      if (definitionA == null || definitionB == null) {
        return definitionA == definitionB ? 0 : (definitionA == null ? -1 : 1);
      }

      int priorityA = definitionA.getPriority().getPriority();
      int priorityB = definitionB.getPriority().getPriority();

      if (priorityA == priorityB) {
        return (int) (definitionA.getArrival() - definitionB.getArrival());
      }

      return priorityA - priorityB;
    }
  }

}
