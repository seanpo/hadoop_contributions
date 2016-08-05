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

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationDefinitionPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationRequestsPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.CapacityOverTimePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.InMemoryPlan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystemTestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.log.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestSimplePriorityAgent {

  PriorityReservationAgent agent;
  InMemoryPlan plan;
  Resource minAlloc = Resource.newInstance(1024, 1);
  ResourceCalculator res = new DefaultResourceCalculator();
  Resource maxAlloc = Resource.newInstance(1024 * 8, 8);
  Random rand = new Random();
  int numContainers = 100;
  long step;

  public TestSimplePriorityAgent() {
  }

  @Before
  public void setup() throws Exception {

    long seed = rand.nextLong();
    rand.setSeed(seed);
    Log.info("Running with seed: " + seed);

    // setting completely loose quotas
    long timeWindow = 1000000L;
    Resource clusterCapacity =
        Resource.newInstance(numContainers * 1024, numContainers);
    String reservationQ =
        ReservationSystemTestUtil.getFullReservationQueueName();

    float instConstraint = 100;
    float avgConstraint = 100;

    ReservationSchedulerConfiguration conf = ReservationSystemTestUtil
        .createConf(reservationQ, timeWindow, instConstraint, avgConstraint);
    CapacityOverTimePolicy policy = new CapacityOverTimePolicy();
    policy.init(reservationQ, conf);

    ReservationAgent workerAgent = new GreedyReservationAgent(conf);
    agent = new SimplePriorityReservationAgent();
    agent.setAgent(workerAgent);

    conf = ReservationSystemTestUtil.createConf(reservationQ, timeWindow,
        instConstraint, avgConstraint);
    policy = new CapacityOverTimePolicy();
    policy.init(reservationQ, conf);
    step = 1000L;

    QueueMetrics queueMetrics = mock(QueueMetrics.class);
    RMContext context = ReservationSystemTestUtil.createMockRMContext();

    plan = new InMemoryPlan(queueMetrics, policy, agent, clusterCapacity, step,
        res, minAlloc, maxAlloc, "dedicated", null, true, context);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testSimple() throws PlanningException {
    boolean result = submitReservation(1, numContainers / 2) != null;

    // validate results, we expect the second one to be accepted
    assertTrue("Reservation should succeed if it can fit.", result);
    assertTrue("The first reservation is expected to succeed.",
        plan.getAllReservations().size() == 1);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testOnlySamePriorityNoFit() throws PlanningException {
    submitReservation(1, numContainers / 2);
    submitReservation(1, numContainers / 2);
    boolean result = submitReservation(1, numContainers / 2) == null;

    // validate results, we expect the second one to be accepted
    assertTrue("Reservation should fail if it cannot fit and there are no "
        + "other reservations that have a lower priority.", result);
    assertTrue("The first two reservations are expected to succeed.",
        plan.getAllReservations().size() == 2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testLowerPriorityExistsNoFit() throws PlanningException {
    ReservationId reservation1 = submitReservation(1, numContainers / 2);
    submitReservation(2, numContainers / 2);
    ReservationId reservation3 = submitReservation(3, numContainers / 2);

    // validate results, we expect the second one to be accepted
    assertTrue("Reservation should succeed because lower priority "
        + "reservations exist.", reservation3 != null);
    assertTrue("The lowest priority reservation should be removed.",
        plan.getReservationById(reservation1) == null);
    assertTrue("There should only be two reservations in the plan.",
        plan.getAllReservations().size() == 2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testMultipleLowerPriorityExistsNoFit() throws PlanningException {
    ReservationId reservation1 = submitReservation(1, numContainers / 2);
    ReservationId reservation2 = submitReservation(2, numContainers / 2);
    ReservationId reservation3 = submitReservation(3, numContainers);

    // validate results, we expect the second one to be accepted
    assertTrue("Reservation should succeed because lower priority "
        + "reservations exist.", reservation3 != null);
    assertTrue("The two lowest priority reservations should be removed.",
        plan.getReservationById(reservation1) == null);
    assertTrue("The two lowest priority reservations should be removed.",
        plan.getReservationById(reservation2) == null);
    assertTrue("There should only be one reservation in the plan.",
        plan.getAllReservations().size() == 1);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testReservationGetsRemovedInPriorityThenArrivalOrder()
      throws PlanningException {
    ReservationId reservation1 =
        submitReservation(1, numContainers / 2, 3 * step, 7 * step, 4 * step);
    submitReservation(1, numContainers / 2, step, 5 * step, 4 * step);
    ReservationId reservation3 =
        submitReservation(3, numContainers / 2, 2 * step, 6 * step, 4 * step);

    // validate results, we expect the second one to be accepted
    assertTrue("Reservation should succeed because lower priority "
        + "reservations exist.", reservation3 != null);
    assertTrue(
        "The reservation with the lowest priority and highest arrival "
            + "time should be removed.",
        plan.getReservationById(reservation1) == null);
    assertTrue("There should only be two reservations in the plan.",
        plan.getAllReservations().size() == 2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testReservationGetsRemovedInPriorityThenArrivalOrderUnlessNoFit()
      throws PlanningException {
    submitReservation(1, (int) (0.4 * numContainers), 3 * step, 7 * step,
        4 * step);
    // This should get removed despite the earlier arrival time because it
    // cannot fit with the higher priority reservation.
    ReservationId reservation2 = submitReservation(1,
        (int) (0.6 * numContainers), step, 5 * step, 4 * step);
    ReservationId reservation3 = submitReservation(3,
        (int) (0.5 * numContainers), 2 * step, 6 * step, 4 * step);

    // validate results, we expect the second one to be accepted
    assertTrue("Reservation should succeed because lower priority "
        + "reservations exist.", reservation3 != null);
    assertTrue(
        "The reservation with the lowest priority and highest arrival "
            + "time should be removed.",
        plan.getReservationById(reservation2) == null);
    assertTrue("There should only be two reservations in the plan.",
        plan.getAllReservations().size() == 2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testOnlyLowerPriorityReservationsGetRemovedRegardlessOfStartTime()
      throws PlanningException {
    // Lower priority than reservation 3, but later start time than the
    // second reservation.
    submitReservation(2, numContainers / 2, 3 * step, 7 * step, 4 * step);

    // Lowest priority, but earliest startTime.
    ReservationId reservation2 =
        submitReservation(1, numContainers / 2, step, 5 * step, 4 * step);
    ReservationId reservation3 =
        submitReservation(3, numContainers / 2, 2 * step, 6 * step, 4 * step);

    // validate results, we expect the second one to be accepted
    assertTrue("Reservation should succeed because lower priority "
        + "reservations exist.", reservation3 != null);
    assertTrue(
        "The reservation with the lowest priority time should be removed "
            + "regardless of the startTime.",
        plan.getReservationById(reservation2) == null);
    assertTrue("There should only be two reservations in the plan.",
        plan.getAllReservations().size() == 2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testOnlyLowerPriorityReservationsGetRemoved()
      throws PlanningException {
    submitReservation(4, numContainers / 2, 3 * step, 7 * step, 4 * step);
    ReservationId reservation2 =
        submitReservation(1, numContainers / 2, step, 5 * step, 4 * step);
    ReservationId reservation3 =
        submitReservation(3, numContainers / 2, 2 * step, 6 * step, 4 * step);

    // validate results, we expect the second one to be accepted
    assertTrue("Reservation should succeed because lower priority "
        + "reservations exist.", reservation3 != null);
    assertTrue("The reservation with the lowest priority should be removed.",
        plan.getReservationById(reservation2) == null);
    assertTrue("There should only be two reservations in the plan.",
        plan.getAllReservations().size() == 2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testLowerPriorityExistsFit() throws PlanningException {
    submitReservation(1, numContainers / 2);
    boolean result = submitReservation(2, numContainers / 2) == null;

    // validate results, we expect the second one to be accepted
    assertFalse("Reservation should succeed if it can fit.", result);
    assertTrue("The first two reservations are expected to succeed.",
        plan.getAllReservations().size() == 2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testOnlyHigherPriorityNoFit() throws PlanningException {
    submitReservation(2, numContainers / 2);
    submitReservation(2, numContainers / 2);
    boolean result = submitReservation(1, numContainers / 2) == null;

    // validate results, we expect the second one to be accepted
    assertTrue("Reservation should fail if it cannot fit and there are no "
        + "other reservations that have a lower priority.", result);
    assertTrue("The first two reservations are expected to succeed",
        plan.getAllReservations().size() == 2);
  }

  private ReservationId submitReservation(int priority, int containers) {
    return submitReservation(priority, containers, step, 2 * step, step);
  }

  private ReservationId submitReservation(int priority, int containers,
      long arrival, long deadline, long duration) {
    // create an ALL request, with an impossible combination, it should be
    // rejected, and allocation remain unchanged
    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(arrival);
    rr.setDeadline(deadline);
    rr.setPriority(priority);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setInterpreter(ReservationRequestInterpreter.R_ALL);
    ReservationRequest r = ReservationRequest
        .newInstance(Resource.newInstance(1024, 1), containers, 1, duration);

    List<ReservationRequest> list = new ArrayList<>();
    list.add(r);
    reqs.setReservationResources(list);
    rr.setReservationRequests(reqs);

    ReservationId reservationID =
        ReservationSystemTestUtil.getNewReservationId();
    try {
      // submit to agent
      agent.createReservation(reservationID, "u1", plan, rr);
      return reservationID;
    } catch (PlanningException p) {
      return null;
    }
  }

}
