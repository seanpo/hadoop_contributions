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

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationPriorityScope;
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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@SuppressWarnings("javadoc")
public class TestSimplePriorityReservationAgent {

  private final String defaultUser = "u1";
  private ReservationSchedulerConfiguration conf;
  private PriorityReservationAgent agent;
  private InMemoryPlan plan;
  private Resource minAlloc = Resource.newInstance(1024, 1);
  private ResourceCalculator res = new DefaultResourceCalculator();
  private Resource maxAlloc = Resource.newInstance(1024 * 8, 8);
  private Random rand = new Random();
  private int numContainers = 100;
  private long step;

  @Before
  public void setup() throws Exception {

    float instConstraint = 100;
    float avgConstraint = 100;
    long timeWindow = 1000000L;
    long seed = rand.nextLong();
    rand.setSeed(seed);
    step = 1000L;

    QueueMetrics queueMetrics = mock(QueueMetrics.class);
    RMContext context = ReservationSystemTestUtil.createMockRMContext();
    Resource clusterCapacity =
        Resource.newInstance(numContainers * 1024, numContainers);
    String reservationQ =
        ReservationSystemTestUtil.getFullReservationQueueName();

    conf = ReservationSystemTestUtil
        .createConf(reservationQ, timeWindow, instConstraint, avgConstraint);

    CapacityOverTimePolicy policy = new CapacityOverTimePolicy();
    policy.init(reservationQ, conf);

    ReservationAgent workerAgent = new GreedyReservationAgent(conf);
    agent = new SimplePriorityReservationAgent(conf);
    agent.setAgent(workerAgent);

    plan = new InMemoryPlan(queueMetrics, policy, agent, clusterCapacity, step,
        res, minAlloc, maxAlloc, "dedicated", null, true, context);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testSubmitSimple() throws PlanningException {
    setPriorityScope(ReservationPriorityScope.QUEUE);
    ReservationId reservation1 = submitReservation(1, numContainers / 2);
    setPriorityScope(ReservationPriorityScope.USER);
    ReservationId reservation2 = submitReservation(1, numContainers / 2);

    // Reservation submission should work with reservation priority enabled.
    assertReservationsInPlan(reservation1, reservation2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testSubmitWithSamePriorityNoFitQueue() throws PlanningException {
    setPriorityScope(ReservationPriorityScope.QUEUE);
    ReservationId reservation1 = submitReservation(1, numContainers / 2);
    ReservationId reservation2 = submitReservation(1, numContainers / 2);
    ReservationId reservation3 = submitReservation(1, numContainers / 2);

    // Verify that only the first two reservations succeed.
    assertReservationsInPlan(reservation1, reservation2);
    assertReservationsNotInPlan(reservation3);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testSubmitWithSamePriorityNoFitUser() throws PlanningException {
    setPriorityScope(ReservationPriorityScope.USER);
    ReservationId reservation1 = submitReservation(1, numContainers / 2);
    ReservationId reservation2 = submitReservation(1, numContainers / 2);
    ReservationId reservation3 = submitReservation(1, numContainers / 2);

    // Verify that only the first two reservations succeed.
    assertReservationsInPlan(reservation1, reservation2);
    assertReservationsNotInPlan(reservation3);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testSubmitWithHigherPriorityNoFitQueue()
      throws PlanningException {
    setPriorityScope(ReservationPriorityScope.QUEUE);
    ReservationId reservation1 = submitReservation(3, numContainers / 2);
    ReservationId reservation2 = submitReservation(2, numContainers / 2);

    // Verify that the first two reservations succeed.
    assertReservationsInPlan(reservation1, reservation2);

    ReservationId reservation3 = submitReservation(1, numContainers / 2, "u4");

    // Verify that the reservations with the highest priority succeed.
    assertReservationsInPlan(reservation2, reservation3);
    assertReservationsNotInPlan(reservation1);
  }
  
  @SuppressWarnings("javadoc")
  @Test
  public void testSubmitWithHigherPriorityNoFitUser() throws PlanningException {
    setPriorityScope(ReservationPriorityScope.USER);
    ReservationId reservation1 = submitReservation(3, numContainers / 2);
    ReservationId reservation2 = submitReservation(2, numContainers / 2, "u2");

    // Verify that the first two reservations succeed.
    assertReservationsInPlan(reservation1, reservation2);

    ReservationId reservation3 = submitReservation(1, numContainers / 2, "u2");

    // Verify that the reservations with the highest priority for each user
    // succeeds.
    assertReservationsInPlan(reservation1, reservation3);
    assertReservationsNotInPlan(reservation2);

    ReservationId reservation4 = submitReservation(1, numContainers / 2);

    // Verify that the reservations with the highest priority for each user
    // succeeds.
    assertReservationsInPlan(reservation3, reservation4);
    assertReservationsNotInPlan(reservation1, reservation2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testSubmitMultipleLowerPriorityExistsNoFitQueue()
      throws PlanningException {
    setPriorityScope(ReservationPriorityScope.QUEUE);
    ReservationId reservation1 = submitReservation(3, numContainers / 2);
    ReservationId reservation2 = submitReservation(2, numContainers / 2);
    ReservationId reservation3 = submitReservation(1, numContainers);

    assertReservationsInPlan(reservation3);
    assertReservationsNotInPlan(reservation1, reservation2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testSubmitMultipleLowerPriorityExistsNoFitUser()
      throws PlanningException {
    setPriorityScope(ReservationPriorityScope.USER);
    ReservationId reservation1 = submitReservation(3, numContainers / 2);
    ReservationId reservation2 = submitReservation(2, numContainers / 2);
    ReservationId reservation3 = submitReservation(1, numContainers, "u2");

    // Ensure priority has no effect on reservations for different user if
    // the scope is set to USER.
    assertReservationsInPlan(reservation1, reservation2);
    assertReservationsNotInPlan(reservation3);

    ReservationId reservation4 = submitReservation(1, numContainers);

    // Since reservation4 is made by the same user that submitted the
    // existing reservations, and is higher priority, other reservations
    // should be yielded.
    assertReservationsInPlan(reservation4);
    assertReservationsNotInPlan(reservation1, reservation2, reservation3);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testSubmitRemovalInPriorityThenArrivalOrderQueue()
      throws PlanningException {
    setPriorityScope(ReservationPriorityScope.QUEUE);
    ReservationId reservation1 =
        submitReservation(2, numContainers / 2, 3 * step, 7 * step, 4 * step);
    ReservationId reservation2 =
        submitReservation(2, numContainers / 2, step, 5 * step, 4 * step);
    ReservationId reservation3 =
        submitReservation(1, numContainers / 2, 2 * step, 6 * step, 4 * step);

    // Reservation2 starts earlier than reservation1, and has the same
    // priority. This means that reservation1 should be the first to be
    // yielded for reservation3.
    assertReservationsInPlan(reservation2, reservation3);
    assertReservationsNotInPlan(reservation1);

    ReservationId reservation4 =
        submitReservation(1, numContainers / 2, 2 * step, 6 * step, 4 * step,
            "u2");

    // Reservation4 yields reservation2, because it is higher priority, even
    // if it is submitted with a different user.
    assertReservationsInPlan(reservation3, reservation4);
    assertReservationsNotInPlan(reservation1, reservation2);

  }

  @SuppressWarnings("javadoc")
  @Test
  public void testSubmitRemovalInPriorityThenArrivalOrderUser()
      throws PlanningException {
    setPriorityScope(ReservationPriorityScope.USER);
    ReservationId reservation1 =
        submitReservation(3, numContainers / 2, 3 * step, 7 * step, 4 * step);
    ReservationId reservation2 =
        submitReservation(3, numContainers / 2, step, 5 * step, 4 * step);
    ReservationId reservation3 =
        submitReservation(2, numContainers / 2, 2 * step, 6 * step, 4 * step);

    // Reservation3 will yield reservation1 because it starts earlier
    assertReservationsInPlan(reservation2, reservation3);
    assertReservationsNotInPlan(reservation1);

    ReservationId reservation4 =
        submitReservation(2, numContainers / 2, 2 * step, 6 * step, 4 * step,
            "u2");

    // Reservation2 starts earlier than reservation1, and has the same
    // priority. This means that reservation1 should be the first to be
    // yielded for reservation3. Reservation4 belongs to a different user, so
    // it cannot yield other reservations.
    assertReservationsInPlan(reservation2, reservation3);
    assertReservationsNotInPlan(reservation1, reservation4);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testSubmitRemovalInArrivalOrderUnlessNoFit()
      throws PlanningException {
    setPriorityScope(ReservationPriorityScope.QUEUE);
    ReservationId reservation1 = submitReservation(
        2, (int) (0.4 * numContainers), 3 * step, 7 * step, 4 * step);
    ReservationId reservation2 = submitReservation(
        2, (int) (0.6 * numContainers), step, 5 * step, 4 * step);
    ReservationId reservation3 = submitReservation(
        1, (int) (0.5 * numContainers), 2 * step, 6 * step, 4 * step, "u2");

    // Reservation2 is deleted despite starting earlier because deleting
    // reservation1 would not have caused reservation3 to fit.
    assertReservationsInPlan(reservation1, reservation3);
    assertReservationsNotInPlan(reservation2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testSubmitRemovalInPriorityOrderUnlessNoFit()
      throws PlanningException {
    setPriorityScope(ReservationPriorityScope.QUEUE);
    ReservationId reservation1 = submitReservation(
        3, (int) (0.4 * numContainers), 3 * step, 7 * step, 4 * step);
    ReservationId reservation2 = submitReservation(
        2, (int) (0.6 * numContainers), step, 5 * step, 4 * step);
    ReservationId reservation3 = submitReservation(
        1, (int) (0.5 * numContainers), 2 * step, 6 * step, 4 * step, "u2");

    // Reservation2 is deleted despite having a higher priority because deleting
    // reservation1 would not have caused reservation3 to fit.
    assertReservationsInPlan(reservation1, reservation3);
    assertReservationsNotInPlan(reservation2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testSubmitLowerPriorityRemovedRegardlessOfStartTime()
      throws PlanningException {
    setPriorityScope(ReservationPriorityScope.QUEUE);
    ReservationId reservation1 =
        submitReservation(2, numContainers / 2, 3 * step, 7 * step, 4 * step);
    ReservationId reservation2 =
        submitReservation(3, numContainers / 2, step, 5 * step, 4 * step);
    ReservationId reservation3 =
        submitReservation(1, numContainers / 2, 2 * step, 6 * step, 4 * step);

    // Reservation2 is deleted despite starting the earliest, because it has
    // lowest priority.
    assertReservationsInPlan(reservation1, reservation3);
    assertReservationsNotInPlan(reservation2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testSubmitOnlyLowerPriorityReservationsGetRemovedQueue()
      throws PlanningException {
    setPriorityScope(ReservationPriorityScope.QUEUE);
    ReservationId reservation1 =
        submitReservation(3, numContainers / 2, 3 * step, 7 * step, 4 * step);
    ReservationId reservation2 =
        submitReservation(1, numContainers / 2, step, 5 * step, 4 * step);
    ReservationId reservation3 =
        submitReservation(2, numContainers / 2, 2 * step, 6 * step, 4 * step);

    // Reservation1 is deleted because it is lowest priority.
    assertReservationsInPlan(reservation2, reservation3);
    assertReservationsNotInPlan(reservation1);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testSubmitOnlyLowerPriorityReservationsGetRemovedUser()
      throws PlanningException {
    setPriorityScope(ReservationPriorityScope.USER);
    ReservationId reservation1 =
        submitReservation(3, numContainers / 2, step, 7 * step, 4 * step, "u2");
    ReservationId reservation2 =
        submitReservation(1, numContainers / 2, step, 5 * step, 4 * step);
    ReservationId reservation3 =
        submitReservation(2, numContainers / 2, 2 * step, 6 * step, 4 * step);

    // Reservation3 is rejected despite there existing a reservation with
    // lower priority because it belongs to a different user.
    assertReservationsInPlan(reservation1, reservation2);
    assertReservationsNotInPlan(reservation3);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testSubmitFailureWithLowerPriorityNoFit()
      throws PlanningException {
    setPriorityScope(ReservationPriorityScope.QUEUE);
    ReservationId reservation1 = submitReservation(2, numContainers / 2);
    ReservationId reservation2 = submitReservation(2, numContainers / 2);
    ReservationId reservation3 = submitReservation(3, numContainers / 2);

    assertReservationsInPlan(reservation1, reservation2);
    assertReservationsNotInPlan(reservation3);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testUpdateSimple() throws PlanningException {
    setPriorityScope(ReservationPriorityScope.QUEUE);
    ReservationId reservation1 = submitReservation(1, numContainers / 2);
    boolean result = updateReservation(reservation1, numContainers);

    assertTrue("Reservation update should succeed if it can fit.", result);
    assertReservationsInPlan(reservation1);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testUpdateWithSamePriorityNoFit() throws PlanningException {
    setPriorityScope(ReservationPriorityScope.QUEUE);
    ReservationId reservation1 = submitReservation(1, numContainers / 2);
    ReservationId reservation2 = submitReservation(1, numContainers / 2);
    boolean result = updateReservation(reservation2, numContainers);

    assertFalse("Reservation update should fail if the new contract will not "
        + "fit and there are no other reservations that have a lower "
        + "priority.", result);
    assertReservationsInPlan(reservation1, reservation2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testUpdateWithHigherPriorityNoFitQueue()
      throws PlanningException {
    setPriorityScope(ReservationPriorityScope.QUEUE);
    ReservationId reservation1 = submitReservation(3, numContainers / 2, "u2");
    ReservationId reservation2 = submitReservation(1, numContainers / 2);
    boolean result = updateReservation(reservation2, numContainers);

    assertTrue("Reservation update should succeed because the reservation is "
        + "highest priority.", result);
    assertReservationsInPlan(reservation2);
    assertReservationsNotInPlan(reservation1);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testUpdateWithHigherPriorityNoFitUser() throws PlanningException {
    setPriorityScope(ReservationPriorityScope.USER);
    ReservationId reservation1 = submitReservation(3, numContainers / 2, "u2");
    ReservationId reservation2 = submitReservation(1, numContainers / 2);
    boolean result = updateReservation(reservation2, numContainers);

    assertFalse("Reservation update should not succeed because the " +
        "increasing the reservation size will cause it not to fit.", result);
    assertReservationsInPlan(reservation1, reservation2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testUpdateMultipleLowerPriorityExistsNoFitQueue()
      throws PlanningException {
    setPriorityScope(ReservationPriorityScope.QUEUE);
    int containers = (int) (0.3 * numContainers);
    ReservationId reservation1 = submitReservation(3, containers);
    ReservationId reservation2 = submitReservation(2, containers, "u2");
    ReservationId reservation3 = submitReservation(1, containers);

    boolean result = updateReservation(reservation3, numContainers);

    assertTrue("Reservation update should succeed because lower priority "
        + "reservations exist.", result);
    assertReservationsInPlan(reservation3);
    assertReservationsNotInPlan(reservation1, reservation2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testUpdateMultipleLowerPriorityExistsNoFitUser()
      throws PlanningException {
    setPriorityScope(ReservationPriorityScope.USER);
    int containers = (int) (0.3 * numContainers);
    ReservationId reservation1 = submitReservation(2, containers);
    ReservationId reservation2 = submitReservation(3, containers, "u2");
    ReservationId reservation3 = submitReservation(1, containers);

    boolean result = updateReservation(reservation3, numContainers);

    assertFalse("Reservation update should not succeed because not " +
        "enough reservations can be yielded to accept the update.", result);
    assertReservationsInPlan(reservation1, reservation2, reservation3);

    result = updateReservation(reservation3, containers * 2);

    assertTrue("Reservation update should succeed because a lower priority "
        + "reservation exist.", result);
    assertReservationsInPlan(reservation2, reservation3);
    assertReservationsNotInPlan(reservation1);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testUpdateRemovalInPriorityThenArrivalOrderQueue()
      throws PlanningException {
    setPriorityScope(ReservationPriorityScope.QUEUE);
    int containers = (int) (0.3 * numContainers);
    ReservationId reservation1 =
        submitReservation(2, containers, step, 5 * step, 4 * step);
    ReservationId reservation2 =
        submitReservation(2, containers, 3 * step, 7 * step, 4 * step);
    ReservationId reservation3 =
        submitReservation(1, containers, 2 * step, 6 * step, 4 * step);

    int newContainers = (int) (0.5 * numContainers);
    boolean result = updateReservation(reservation3, newContainers);

    assertTrue("Reservation update should succeed because lower priority "
        + "reservations exist.", result);
    assertReservationsInPlan(reservation1, reservation3);
    assertReservationsNotInPlan(reservation2);
  }

  @Test
  public void testUpdateRemovalInPriorityThenArrivalOrderUser()
      throws PlanningException {
    setPriorityScope(ReservationPriorityScope.USER);
    int containers = (int) (0.3 * numContainers);
    ReservationId reservation1 =
        submitReservation(2, containers, step, 5 * step, 4 * step);
    ReservationId reservation2 =
        submitReservation(2, containers, 3 * step, 7 * step, 4 * step, "u2");
    ReservationId reservation3 =
        submitReservation(1, containers, 2 * step, 6 * step, 4 * step);

    int newContainers = (int) (0.5 * numContainers);
    boolean result = updateReservation(reservation3, newContainers);

    assertTrue("Reservation update should succeed because lower priority "
        + "reservations exist.", result);
    assertReservationsInPlan(reservation2, reservation3);
    assertReservationsNotInPlan(reservation1);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testUpdateRemovalInPriorityThenArrivalOrderUnlessNoFit()
      throws PlanningException {
    setPriorityScope(ReservationPriorityScope.QUEUE);
    ReservationId reservation1 = submitReservation(
        2, (int) (0.2 * numContainers), 3 * step, 7 * step, 4 * step);
    // This should get removed despite the earlier arrival time because it
    // cannot fit with the higher priority reservation.
    ReservationId reservation2 = submitReservation(
        2, (int) (0.6 * numContainers), step, 5 * step, 4 * step);
    ReservationId reservation3 = submitReservation(
        1, (int) (0.2 * numContainers), 2 * step, 6 * step, 4 * step);

    boolean result =
        updateReservation(reservation3, (int) (0.7 * numContainers));

    // validate results, we expect the second one to be accepted
    assertTrue("Reservation update should succeed because lower priority "
        + "reservations exist.", result);
    assertReservationsInPlan(reservation1, reservation3);
    assertReservationsNotInPlan(reservation2);

  }

  @SuppressWarnings("javadoc")
  @Test
  public void testUpdateLowerPriorityRemovedRegardlessOfStartTime()
      throws PlanningException {
    setPriorityScope(ReservationPriorityScope.QUEUE);
    int containers = (int) (0.3 * numContainers);

    // Lower priority than reservation 3, but later start time than the
    // second reservation.
    ReservationId reservation1 =
        submitReservation(2, containers, 3 * step, 7 * step, 4 * step);
    // Lowest priority, but earliest startTime.
    ReservationId reservation2 =
        submitReservation(3, containers, step, 5 * step, 4 * step);
    ReservationId reservation3 =
        submitReservation(1, containers, 2 * step, 6 * step, 4 * step);

    boolean result = updateReservation(reservation3, numContainers / 2);

    assertTrue("Reservation update should succeed because lower priority "
        + "reservations exist.", result);
    assertReservationsInPlan(reservation1, reservation3);
    assertReservationsNotInPlan(reservation2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testUpdateOnlyLowerPriorityReservationsGetRemoved()
      throws PlanningException {
    setPriorityScope(ReservationPriorityScope.QUEUE);
    int containers = (int) (0.3 * numContainers);

    ReservationId reservation1 =
        submitReservation(1, containers, 3 * step, 7 * step, 4 * step);
    ReservationId reservation2 =
        submitReservation(3, containers, step, 5 * step, 4 * step);
    ReservationId reservation3 =
        submitReservation(2, containers, 2 * step, 6 * step, 4 * step);

    boolean result = updateReservation(reservation3, numContainers / 2);

    assertTrue("Reservation update should succeed because lower priority "
        + "reservations exist.", result);
    assertReservationsInPlan(reservation1, reservation3);
    assertReservationsNotInPlan(reservation2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testUpdateSuccessWithHigherPriorityFit()
      throws PlanningException {
    setPriorityScope(ReservationPriorityScope.QUEUE);
    ReservationId reservation1 = submitReservation(2, numContainers / 2);
    ReservationId reservation2 = submitReservation(1, numContainers / 3);

    boolean result = updateReservation(reservation2, numContainers / 2);

    // validate results, we expect the second one to be accepted
    assertTrue("Reservation should succeed if it can fit.", result);
    assertReservationsInPlan(reservation1, reservation2);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testUpdateFailureWithLowerPriorityNoFit()
      throws PlanningException {
    setPriorityScope(ReservationPriorityScope.QUEUE);
    ReservationId reservation1 = submitReservation(1, numContainers / 2);
    ReservationId reservation2 = submitReservation(2, numContainers / 2);

    boolean result = updateReservation(reservation2, numContainers);

    // validate results, we expect the second one to be accepted
    assertFalse("Reservation should fail if it cannot fit and there are no "
        + "other reservations that have a lower priority.", result);
    assertReservationsInPlan(reservation1, reservation2);
  }

  private void setPriorityScope(ReservationPriorityScope scope) {
    conf.setEnum(CapacitySchedulerConfiguration.RESERVATION_PRIORITY_SCOPE,
        scope);
    agent.setConf(conf);
  }

  private void assertReservationsNotInPlan(ReservationId... reservationIds) {
    for (ReservationId reservationId : reservationIds) {
      assertTrue("Reservation ID " + reservationId + " shouldn't exist in plan",
          plan.getReservationById(reservationId) == null);
    }
  }

  private void assertReservationsInPlan(ReservationId... reservationIds) {
    for (ReservationId reservationId : reservationIds) {
      assertTrue("Reservation ID " + reservationId + " should exist in plan",
          plan.getReservationById(reservationId) != null);
    }
  }

  private ReservationId submitReservation(int priority, int containers) {
    return submitReservation(priority, containers, step, 2 * step, step,
        defaultUser);
  }

  private ReservationId submitReservation(int priority, int containers,
      String user) {
    return submitReservation(priority, containers, step, 2 * step, step, user);
  }

  private ReservationId submitReservation(int priority, int containers,
      long arrival, long deadline, long duration) {
    return submitReservation(priority, containers, arrival, deadline, duration,
        defaultUser);
  }

  private ReservationId submitReservation(int priority, int containers,
      long arrival, long deadline, long duration, String user) {
    // create an ALL request, with an impossible combination, it should be
    // rejected, and allocation remain unchanged
    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(arrival);
    rr.setDeadline(deadline);
    rr.setPriority(Priority.newInstance(priority));
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
      agent.createReservation(reservationID, user, plan, rr);
    } catch (PlanningException p) {
      p.printStackTrace();
    }
    return reservationID;
  }

  private boolean updateReservation(ReservationId reservationId,
      int containers) {
    // Create an ALL request, with an impossible combination, it should be
    // rejected, and allocation remain unchanged
    ReservationDefinition oldContract =
        plan.getReservationById(reservationId).getReservationDefinition();
    ReservationRequests requests = new ReservationRequestsPBImpl();
    requests.setInterpreter(ReservationRequestInterpreter.R_ALL);
    // This should always succeed because reservations that are created in this
    // test only have one reservation request.
    ReservationRequest r =
        oldContract.getReservationRequests().getReservationResources().get(0);
    r.setNumContainers(containers);

    List<ReservationRequest> list = new ArrayList<>();
    list.add(r);
    requests.setReservationResources(list);
    oldContract.setReservationRequests(requests);

    try {
      return agent.updateReservation(reservationId, "u1", plan, oldContract);
    } catch (PlanningException p) {
      return false;
    }
  }

}
