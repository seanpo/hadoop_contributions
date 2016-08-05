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
  public void testSubmitSimple() throws PlanningException {
    boolean result = submitReservation(1, numContainers / 2) != null;

    // validate results, we expect the second one to be accepted
    assertTrue("Reservation should succeed if it can fit.", result);
    assertTrue("The first reservation is expected to succeed.",
        plan.getAllReservations().size() == 1);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testSubmitWithSamePriorityNoFit() throws PlanningException {
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
  public void testSubmitWithHigerPriorityNoFit() throws PlanningException {
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
  public void testSubmitMultipleLowerPriorityExistsNoFit()
      throws PlanningException {
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
  public void testSubmitRemovalInPriorityThenArrivalOrder()
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
  public void testSubmitRemovalInPriorityThenArrivalOrderUnlessNoFit()
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
  public void testSubmitLowerPriorityRemovedRegardlessOfStartTime()
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
  public void testSubmitOnlyLowerPriorityReservationsGetRemoved()
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
  public void testSubmitSuccessWithHigherPriorityFit()
      throws PlanningException {
    submitReservation(1, numContainers / 2);
    boolean result = submitReservation(2, numContainers / 2) == null;

    // validate results, we expect the second one to be accepted
    assertFalse("Reservation should succeed if it can fit.", result);
    assertTrue("The first two reservations are expected to succeed.",
        plan.getAllReservations().size() == 2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testSubmitFailureWithLowerPriorityNoFit()
      throws PlanningException {
    submitReservation(2, numContainers / 2);
    submitReservation(2, numContainers / 2);
    boolean result = submitReservation(1, numContainers / 2) == null;

    // validate results, we expect the second one to be accepted
    assertTrue("Reservation should fail if it cannot fit and there are no "
        + "other reservations that have a lower priority.", result);
    assertTrue("The first two reservations are expected to succeed",
        plan.getAllReservations().size() == 2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testUpdateSimple() throws PlanningException {
    ReservationId reservation1 = submitReservation(1, numContainers / 2);
    boolean result = updateReservation(reservation1, numContainers);

    // validate results, we expect the second one to be accepted
    assertTrue("Reservation update should succeed if it can fit.", result);
    assertTrue("The first reservation is expected to succeed.",
        plan.getAllReservations().size() == 1);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testUpdateWithSamePriorityNoFit() throws PlanningException {
    submitReservation(1, numContainers / 2);
    ReservationId reservation2 = submitReservation(1, numContainers / 2);
    boolean result = updateReservation(reservation2, numContainers);

    // validate results, we expect the second one to be accepted
    assertFalse(
        "Reservation update should fail if the new contract will not "
            + "fit and there are no other reservations that have a lower priority.",
        result);
    assertTrue("The first two reservations are expected to succeed.",
        plan.getAllReservations().size() == 2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testUpdateWithHigherPriorityNoFit() throws PlanningException {
    ReservationId reservation1 = submitReservation(1, numContainers / 2);
    ReservationId reservation2 = submitReservation(3, numContainers / 2);
    boolean result = updateReservation(reservation2, numContainers);

    // validate results, we expect the second one to be accepted
    assertTrue("Reservation update should succeed because the reservation is "
        + "highest priority.", result);
    assertTrue("The lowest priority reservation should be removed.",
        plan.getReservationById(reservation1) == null);
    assertTrue("There should only be 1 reservations in the plan.",
        plan.getAllReservations().size() == 1);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testUpdateMultipleLowerPriorityExistsNoFit()
      throws PlanningException {
    int containers = (int) (0.3 * numContainers);
    ReservationId reservation1 = submitReservation(1, containers);
    ReservationId reservation2 = submitReservation(2, containers);
    ReservationId reservation3 = submitReservation(3, containers);

    boolean result = updateReservation(reservation3, numContainers);

    // validate results, we expect the second one to be accepted
    assertTrue("Reservation update should succeed because lower priority "
        + "reservations exist.", result);
    assertTrue("The two lowest priority reservations should be removed.",
        plan.getReservationById(reservation1) == null);
    assertTrue("The two lowest priority reservations should be removed.",
        plan.getReservationById(reservation2) == null);
    assertTrue("There should only be one reservation in the plan.",
        plan.getAllReservations().size() == 1);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testUpdateRemovalInPriorityThenArrivalOrder()
      throws PlanningException {
    int containers = (int) (0.3 * numContainers);
    ReservationId reservation1 =
        submitReservation(1, containers, 3 * step, 7 * step, 4 * step);
    submitReservation(1, containers, step, 5 * step, 4 * step);
    ReservationId reservation3 =
        submitReservation(3, containers, 2 * step, 6 * step, 4 * step);

    int newContainers = (int) (0.5 * numContainers);
    boolean result = updateReservation(reservation3, newContainers);

    // validate results, we expect the second one to be accepted
    assertTrue("Reservation update should succeed because lower priority "
        + "reservations exist.", result);
    assertTrue(
        "The reservation with the lowest priority and highest arrival "
            + "time should be removed.",
        plan.getReservationById(reservation1) == null);
    assertTrue("There should only be two reservations in the plan.",
        plan.getAllReservations().size() == 2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testUpdateRemovalInPriorityThenArrivalOrderUnlessNoFit()
      throws PlanningException {
    submitReservation(1, (int) (0.2 * numContainers), 3 * step, 7 * step,
        4 * step);
    // This should get removed despite the earlier arrival time because it
    // cannot fit with the higher priority reservation.
    ReservationId reservation2 = submitReservation(1,
        (int) (0.6 * numContainers), step, 5 * step, 4 * step);
    ReservationId reservation3 = submitReservation(3,
        (int) (0.2 * numContainers), 2 * step, 6 * step, 4 * step);

    boolean result =
        updateReservation(reservation3, (int) (0.7 * numContainers));

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
  public void testUpdateLowerPriorityRemovedRegardlessOfStartTime()
      throws PlanningException {
    int containers = (int) (0.3 * numContainers);

    // Lower priority than reservation 3, but later start time than the
    // second reservation.
    submitReservation(2, containers, 3 * step, 7 * step, 4 * step);

    // Lowest priority, but earliest startTime.
    ReservationId reservation2 =
        submitReservation(1, containers, step, 5 * step, 4 * step);
    ReservationId reservation3 =
        submitReservation(3, containers, 2 * step, 6 * step, 4 * step);

    boolean result = updateReservation(reservation3, numContainers / 2);

    // validate results, we expect the second one to be accepted
    assertTrue("Reservation update should succeed because lower priority "
        + "reservations exist.", result);
    assertTrue(
        "The reservation with the lowest priority time should be removed "
            + "regardless of the startTime.",
        plan.getReservationById(reservation2) == null);
    assertTrue("There should only be two reservations in the plan.",
        plan.getAllReservations().size() == 2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testUpdateOnlyLowerPriorityReservationsGetRemoved()
      throws PlanningException {
    int containers = (int) (0.3 * numContainers);

    submitReservation(4, containers, 3 * step, 7 * step, 4 * step);
    ReservationId reservation2 =
        submitReservation(1, containers, step, 5 * step, 4 * step);
    ReservationId reservation3 =
        submitReservation(3, containers, 2 * step, 6 * step, 4 * step);

    boolean result = updateReservation(reservation3, numContainers / 2);

    // validate results, we expect the second one to be accepted
    assertTrue("Reservation update should succeed because lower priority "
        + "reservations exist.", result);
    assertTrue("The reservation with the lowest priority should be removed.",
        plan.getReservationById(reservation2) == null);
    assertTrue("There should only be two reservations in the plan.",
        plan.getAllReservations().size() == 2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testUpdateSuccessWithHigherPriorityFit()
      throws PlanningException {
    submitReservation(1, numContainers / 2);
    ReservationId reservationId2 = submitReservation(2, numContainers / 3);

    boolean result = updateReservation(reservationId2, numContainers / 2);

    // validate results, we expect the second one to be accepted
    assertTrue("Reservation should succeed if it can fit.", result);
    assertTrue("The first two reservations are expected to succeed.",
        plan.getAllReservations().size() == 2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testUpdateFailureWithLowerPriorityNoFit()
      throws PlanningException {
    submitReservation(2, numContainers / 2);
    ReservationId reservationId2 = submitReservation(1, numContainers / 2);

    boolean result = updateReservation(reservationId2, numContainers);

    // validate results, we expect the second one to be accepted
    assertFalse("Reservation should fail if it cannot fit and there are no "
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

  private boolean updateReservation(ReservationId reservationId,
      int containers) {
    // Create an ALL request, with an impossible combination, it should be
    // rejected, and allocation remain unchanged
    ReservationDefinition oldContract =
        plan.getReservationById(reservationId).getReservationDefinition();
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setInterpreter(ReservationRequestInterpreter.R_ALL);
    // This should always succeed because reservations that are created in this
    // test only have one reservation request.
    ReservationRequest r =
        oldContract.getReservationRequests().getReservationResources().get(0);
    r.setNumContainers(containers);

    List<ReservationRequest> list = new ArrayList<>();
    list.add(r);
    reqs.setReservationResources(list);
    oldContract.setReservationRequests(reqs);

    try {
      // submit to agent
      return agent.updateReservation(reservationId, "u1", plan, oldContract);
    } catch (PlanningException p) {
      return false;
    }
  }

}
