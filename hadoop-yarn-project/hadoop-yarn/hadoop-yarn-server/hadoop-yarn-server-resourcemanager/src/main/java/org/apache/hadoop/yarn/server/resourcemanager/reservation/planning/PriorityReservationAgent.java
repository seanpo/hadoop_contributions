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
 *******************************************************************************/

package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;

import java.util.List;

public abstract class PriorityReservationAgent implements ReservationAgent {

  private ReservationAgent agent;

  private static final Log LOG =
      LogFactory.getLog(PriorityReservationAgent.class.getName());

  /**
   * Accommodate for an incoming reservation by attempting to remove other
   * reservations in the queue.
   *
   * @param reservationId the identifier of the reservation to be
   *                      accommodated for.
   * @param user the user who the reservation belongs to
   * @param plan the Plan to which the reservation must be fitted
   * @param contract encapsulates the resources the user requires for his
   *          reservation
   *
   * @return an ordered list of {@link ReservationAllocation} that were
   * removed in order to fit the incoming reservation.
   * @throws PlanningException if the reservation cannot be fitted into the plan
   */
  public abstract List<ReservationAllocation> accommodateForReservation(
      ReservationId reservationId, String user, Plan plan,
      ReservationDefinition contract) throws PlanningException;

  public boolean createReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {
    try {
      agent.createReservation(reservationId, user, plan, contract);
      return true;
    } catch (PlanningException e) {
      LOG.info("Encountered planning exception for reservation=[" +
          reservationId.toString() + "] in plan=[" + plan.getQueueName() + "]" +
          " when creating the reservation. Attempt to accommodate for " +
          "reservation by removing lower priority reservations. Exception=[" +
          e.getMessage() + "]");
    }
    List<ReservationAllocation> yieldedReservations =
        accommodateForReservation(reservationId, user, plan, contract);

    try {
      return getAgent().createReservation(reservationId, user, plan, contract);
    } catch (PlanningException e) {
      LOG.info("Reservation=[" + reservationId + "] could not be added even " +
          "after removing lower priority reservations. Attempt to re-add the " +
          "removed reservations.");
      throw e;
    } finally {
      // Add the reservations in the reverse order that they were removed.
      for (ReservationAllocation reservation : yieldedReservations) {
        try {
          agent.createReservation(reservation.getReservationId(),
              reservation.getUser(), plan, reservation
                  .getReservationDefinition());
        } catch(PlanningException e) {
          LOG.info("Reservation=[" + reservation.getReservationId() + "] was " +
              "removed to make room for a higher priority reservation=[" +
              reservationId + "].");
        }
      }
    }
  }

  public boolean updateReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {
    try {
      agent.updateReservation(reservationId, user, plan, contract);
      return true;
    } catch (PlanningException e) {
      LOG.info("Encountered planning exception for reservation=[" +
          reservationId.toString() + "] in plan=[" + plan.getQueueName() + "]" +
          " when updating the reservation. Attempt to accommodate for " +
          "reservation by removing lower priority reservations. Exception=[" +
          e.getMessage() + "]");
    }
    List<ReservationAllocation> yieldedReservations =
        accommodateForReservation(reservationId, user, plan, contract);

    // Add the reservations in the reverse order that they were removed.
    for (ReservationAllocation reservation : yieldedReservations) {
      agent.createReservation(reservation.getReservationId(),
          reservation.getUser(), plan, reservation.getReservationDefinition());
    }
    return true;

  }

  public boolean deleteReservation(ReservationId reservationId, String user,
      Plan plan) throws PlanningException {
    return agent.deleteReservation(reservationId, user, plan);
  }

  public ReservationAgent getAgent() {
    return agent;
  }

  public void setAgent(ReservationAgent newAgent) {
    agent = newAgent;
  }

}
