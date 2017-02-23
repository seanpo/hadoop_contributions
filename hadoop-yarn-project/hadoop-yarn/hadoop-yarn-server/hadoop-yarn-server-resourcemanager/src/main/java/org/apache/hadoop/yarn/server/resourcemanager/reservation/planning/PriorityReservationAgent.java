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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This {@link ReservationAgent} is an abstract agent that wraps other
 * ReservationAgents to make them priority aware.
 *
 * {@link PriorityReservationAgent} will attempt to interact with the plan using
 * the inner {@link ReservationAgent}. If this fails, it will attempt to make
 * room for the reservation based on the method defined in the
 * PriorityReservationAgent subclass.
 */
public abstract class PriorityReservationAgent
    implements ReservationAgent, Configurable {

  private ReservationAgent agent;

  private final ReentrantReadWriteLock readWriteLock =
      new ReentrantReadWriteLock();
  private final Lock writeLock = readWriteLock.writeLock();

  private static final Log LOG =
      LogFactory.getLog(PriorityReservationAgent.class.getName());

  /**
   * Make room for an incoming reservation by attempting to remove other
   * reservations in the queue.
   *
   * @param reservationId the identifier of the reservation to be make roomd
   *          for.
   * @param user the user who the reservation belongs to
   * @param plan the Plan to which the reservation must be fitted
   * @param contract encapsulates the resources the user requires for his
   *          reservation
   *
   * @return an ordered list of {@link ReservationAllocation} that were removed
   *         to fit the incoming reservation. The order of the list determines
   *         the order of reservations that will be recreated.
   * @throws PlanningException if the reservation cannot be fitted into the plan
   */
  public abstract List<ReservationAllocation> makeRoomForReservation(
      ReservationId reservationId, String user, Plan plan,
      ReservationDefinition contract) throws PlanningException;

  public boolean createReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {
    writeLock.lock();
    try {
      agent.createReservation(reservationId, user, plan, contract);
      return true;
    } catch (PlanningException e) {
      LOG.info("Encountered planning exception for reservation=["
          + reservationId.toString() + "] in plan=[" + plan.getQueueName() + "]"
          + " when creating the reservation. Attempt to make room for "
          + "reservation by removing lower priority reservations. Exception=["
          + e.getMessage() + "]");
    }
    List<ReservationAllocation> yieldedReservations =
        makeRoomForReservation(reservationId, user, plan, contract);

    try {
      boolean success =
          agent.createReservation(reservationId, user, plan, contract);
      addYieldedReservations(yieldedReservations, plan, reservationId);
      return success;
    } catch (PlanningException e) {
      // Reset the plan back to its original state.
      addYieldedReservations(yieldedReservations, plan, reservationId, true);
      LOG.info("Reservation=[" + reservationId + "] could not be added even "
          + "after removing lower priority reservations. Attempt to re-add the "
          + "removed reservations.");
      throw e;
    } finally {
      writeLock.unlock();
    }
  }

  public boolean updateReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {
    writeLock.lock();
    try {
      agent.updateReservation(reservationId, user, plan, contract);
      return true;
    } catch (PlanningException e) {
      LOG.info("Encountered planning exception for reservation=["
          + reservationId.toString() + "] in plan=[" + plan.getQueueName() + "]"
          + " when creating the reservation. Attempt to make room for "
          + "reservation by removing lower priority reservations. Exception=["
          + e.getMessage() + "]");
    }
    List<ReservationAllocation> yieldedReservations =
        makeRoomForReservation(reservationId, user, plan, contract);

    try {
      boolean success =
          agent.updateReservation(reservationId, user, plan, contract);
      addYieldedReservations(yieldedReservations, plan, reservationId);
      return success;
    } catch (PlanningException e) {
      addYieldedReservations(yieldedReservations, plan, reservationId, true);
      LOG.info("Reservation=[" + reservationId + "] could not be added even "
          + "after removing lower priority reservations. Attempt to re-add the "
          + "removed reservations.");
      throw e;
    } finally {
      writeLock.unlock();
    }
  }

  private void addYieldedReservations(List<ReservationAllocation> reservations,
      Plan plan, ReservationId reservationId) {
    addYieldedReservations(reservations, plan, reservationId, false);
  }

  private void addYieldedReservations(List<ReservationAllocation> reservations,
      Plan plan, ReservationId reservationId, boolean addInOrderOfAcceptance) {
    if (addInOrderOfAcceptance) {
      // Order by arrival time.
      reservations.sort(new ArrivalTimeComparator());
    }

    for (ReservationAllocation reservation : reservations) {
      try {
        agent.createReservation(reservation.getReservationId(),
            reservation.getUser(), plan,
            reservation.getReservationDefinition());
      } catch (PlanningException e) {
        LOG.info("Reservation=[" + reservation.getReservationId() + "] was "
            + "removed to make room for a higher priority reservation=["
            + reservationId + "].");
      }
    }
  }

  public boolean deleteReservation(ReservationId reservationId, String user,
      Plan plan) throws PlanningException {
    writeLock.lock();
    try {
      return agent.deleteReservation(reservationId, user, plan);
    } finally {
      writeLock.unlock();
    }
  }

  public ReservationAgent getAgent() {
    return agent;
  }

  public void setAgent(ReservationAgent newAgent) {
    agent = newAgent;
  }

  private static class ArrivalTimeComparator
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

      return (int) (definitionA.getArrival() - definitionB.getArrival());
    }
  }

}
