/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This agent wraps the implementation of {@link ReservationAgent} to provide
 * common functionality. An example is logging, and gathering metrics.
 */
abstract class AbstractReservationAgent implements ReservationAgent {

  private ReservationAgentMetrics reservationAgentMetrics;

  protected AbstractReservationAgent() {
    reservationAgentMetrics = ReservationAgentMetrics.getMetrics();
  }

  protected abstract Logger getLogger();

  protected abstract boolean createReservationImpl(ReservationId reservationId,
      String user, Plan plan, ReservationDefinition contract)
      throws PlanningException;

  @Override
  public boolean createReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {

    getLogger().info("placing the following ReservationRequest: " + contract);

    StopWatch stopWatch = new StopWatch();
    stopWatch.start();

    try {
      boolean res = createReservationImpl(reservationId, user, plan, contract);

      if (res) {
        getLogger().info("OUTCOME: SUCCESS, Reservation ID: "
            + reservationId.toString() + ", Contract: " + contract.toString());
      } else {
        getLogger().info("OUTCOME: FAILURE, Reservation ID: "
            + reservationId.toString() + ", Contract: " + contract.toString());
      }

      reservationAgentMetrics.setAgentCreateReservationMetrics(stopWatch.now(),
          res);
      return res;
    } catch (PlanningException e) {
      getLogger().info("OUTCOME: FAILURE, Reservation ID: "
          + reservationId.toString() + ", Contract: " + contract.toString());
      reservationAgentMetrics.setAgentCreateReservationMetrics(stopWatch.now(),
          false);
      throw e;
    }
  }

  protected abstract boolean updateReservationImpl(ReservationId reservationId,
      String user, Plan plan, ReservationDefinition contract)
      throws PlanningException;

  @Override
  public boolean updateReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {

    getLogger().info("updating the following ReservationRequest: " + contract);

    StopWatch stopWatch = new StopWatch();
    stopWatch.start();

    try {
      boolean status =
          updateReservationImpl(reservationId, user, plan, contract);
      reservationAgentMetrics.setAgentUpdateReservationMetrics(stopWatch.now(),
          status);
      return status;
    } catch (PlanningException e) {
      getLogger().info("OUTCOME: FAILURE, Reservation ID: "
          + reservationId.toString() + ", Contract: " + contract.toString());
      reservationAgentMetrics.setAgentUpdateReservationMetrics(stopWatch.now(),
          false);
      throw e;
    }
  }

  public abstract boolean deleteReservationImpl(ReservationId reservationId,
      String user, Plan plan) throws PlanningException;

  @Override
  public boolean deleteReservation(ReservationId reservationId, String user,
      Plan plan) throws PlanningException {

    getLogger().info("removing the following ReservationId: " + reservationId);

    StopWatch stopWatch = new StopWatch();
    stopWatch.start();

    try {
      boolean status = deleteReservationImpl(reservationId, user, plan);
      reservationAgentMetrics.setAgentDeleteReservationMetrics(stopWatch.now(),
          status);
      return status;
    } catch (PlanningException e) {
      getLogger().info(
          "OUTCOME: FAILURE, Reservation ID: " + reservationId.toString());
      reservationAgentMetrics.setAgentDeleteReservationMetrics(stopWatch.now(),
          false);
      throw e;
    }
  }

}
