/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.spark.testsuite.secondaryindex

import mockit.{Mock, MockUp}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.secondaryindex.joins.BroadCastSIFilterPushJoin

import org.apache.carbondata.core.locks.AbstractCarbonLock

object TestSecondaryIndexUtils {
  /**
   * Method to check whether the filter is push down to SI table or not
   *
   * @param sparkPlan
   * @return
   */
  def isFilterPushedDownToSI(sparkPlan: SparkPlan): Boolean = {
    var isValidPlan = false
    sparkPlan.transform {
      case broadCastSIFilterPushDown: BroadCastSIFilterPushJoin =>
        isValidPlan = true
        broadCastSIFilterPushDown
    }
    isValidPlan
  }

  def mockTableLock(): MockUp[AbstractCarbonLock] = {
    val mock: MockUp[AbstractCarbonLock] = new MockUp[AbstractCarbonLock]() {
      @Mock
      def lockWithRetries(): Boolean = {
        false
      }
    }
    mock
  }
}
