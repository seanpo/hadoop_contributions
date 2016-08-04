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
import org.apache.hadoop.yarn.server.resourcemanager.reservation.InMemoryReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystemTestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.log.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
  boolean allocateLeft;

  public TestSimplePriorityAgent(){
  }

  @Before
  public void setup() throws Exception {

    long seed = rand.nextLong();
    rand.setSeed(seed);
    Log.info("Running with seed: " + seed);

    // setting completely loose quotas
    long timeWindow = 1000000L;
    Resource clusterCapacity = Resource.newInstance(numContainers * 1024,
        numContainers);
    String reservationQ =
        ReservationSystemTestUtil.getFullReservationQueueName();

    float instConstraint = 100;
    float avgConstraint = 100;

    ReservationSchedulerConfiguration conf =
        ReservationSystemTestUtil.createConf(reservationQ, timeWindow,
            instConstraint, avgConstraint);
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
    // create an ALL request, with an impossible combination, it should be
    // rejected, and allocation remain unchanged
    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(step);
    rr.setDeadline(2*step);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setInterpreter(ReservationRequestInterpreter.R_ALL);
    ReservationRequest r = ReservationRequest.newInstance(
        Resource.newInstance(1024, 1), numContainers/2, 5, step);

    List<ReservationRequest> list = new ArrayList<ReservationRequest>();
    list.add(r);
    reqs.setReservationResources(list);
    rr.setReservationRequests(reqs);
    rr.setPriority(10);

    ReservationId reservationID = ReservationSystemTestUtil
        .getNewReservationId();
    boolean result = false;
    try {
      // submit to agent
      result = agent.createReservation(reservationID, "u1", plan, rr);
    } catch (PlanningException p) {
      fail();
    }

    // validate results, we expect the second one to be accepted
    assertTrue("Agent-based allocation succeeded", result);
    assertTrue("Agent-based allocation succeeded", plan.getAllReservations()
        .size() == 1);

    System.out.println("--------AFTER ALL IMPOSSIBLE ALLOCATION (queue: "
        + reservationID + ")----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testSamePriorityNoFit() throws PlanningException {
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testLowerPriorityNoFit() throws PlanningException {
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testLowerPriorityFit() throws PlanningException {
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testHigherPriorityNoFit() throws PlanningException {
  }

  private boolean check(ReservationAllocation cs, long start, long end,
      int containers, int mem, int cores) {

    boolean res = true;
    for (long i = start; i < end; i++) {
      res = res
          && Resources.equals(cs.getResourcesAtTime(i),
              Resource.newInstance(mem * containers, cores * containers));
    }
    return res;
  }

}
