/*
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
package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.planner.calcite.FlinkTypeFactory.isRowtimeIndicatorType
import org.apache.flink.table.planner.hint.JoinStrategy
import org.apache.flink.table.planner.plan.nodes.FlinkRelNode
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalIntervalJoin
import org.apache.flink.table.planner.plan.utils.IntervalJoinUtil.satisfyIntervalJoin
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode

import java.util
import scala.collection.JavaConversions._
import scala.collection.JavaConverters.mapAsScalaMapConverter

/**
 * Rule that converts non-SEMI/ANTI [[FlinkLogicalJoin]] with window bounds in join condition to
 * [[StreamPhysicalIntervalJoin]].
 */
class StreamPhysicalIntervalJoinRule
  extends StreamPhysicalJoinRuleBase("StreamPhysicalIntervalJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: FlinkLogicalJoin = call.rel(0)

    if (!satisfyIntervalJoin(join)) {
      return false
    }

    // validate the join
    val windowBounds = extractWindowBounds(join)._1.get

    if (windowBounds.isEventTime) {
      val leftTimeAttributeType = join.getLeft.getRowType.getFieldList
        .get(windowBounds.getLeftTimeIdx)
        .getType
      val rightTimeAttributeType = join.getRight.getRowType.getFieldList
        .get(windowBounds.getRightTimeIdx)
        .getType
      if (leftTimeAttributeType.getSqlTypeName != rightTimeAttributeType.getSqlTypeName) {
        throw new ValidationException(
          String.format(
            "Interval join with rowtime attribute requires same rowtime types," +
              " but the types are %s and %s.",
            leftTimeAttributeType.toString,
            rightTimeAttributeType.toString
          ))
      }
    } else {
      // Check that no event-time attributes are in the input because the processing time window
      // join does not correctly hold back watermarks.
      // We rely on projection pushdown to remove unused attributes before the join.
      val joinRowType = join.getRowType
      val containsRowTime = joinRowType.getFieldList.exists(f => isRowtimeIndicatorType(f.getType))
      if (containsRowTime) {
        throw new TableException(
          "Interval join with proctime attribute requires no event-time attributes are in the " +
            "join inputs.")
      }
    }
    true
  }

  override protected def computeJoinLeftKeys(join: FlinkLogicalJoin): util.Collection[Integer] = {
    val (windowBounds, _) = extractWindowBounds(join)
    join
      .analyzeCondition()
      .leftKeys
      .filter(k => windowBounds.get.getLeftTimeIdx != k)
      .toList
  }

  override protected def computeJoinRightKeys(join: FlinkLogicalJoin): util.Collection[Integer] = {
    val (windowBounds, _) = extractWindowBounds(join)
    join
      .analyzeCondition()
      .rightKeys
      .filter(k => windowBounds.get.getRightTimeIdx != k)
      .toList
  }

  override protected def transform(
      join: FlinkLogicalJoin,
      leftInput: FlinkRelNode,
      leftConversion: RelNode => RelNode,
      rightInput: FlinkRelNode,
      rightConversion: RelNode => RelNode,
      providedTraitSet: RelTraitSet): FlinkRelNode = {
    val (windowBounds, remainCondition) = extractWindowBounds(join)
    // Extract the early fire hint values
    val joinHints = join.getHints
    val earlyFireHint = joinHints.find(hint => JoinStrategy.isEarlyFireHint(hint.hintName))
    val (earlyFireDelay, earlyFireFrequency) = earlyFireHint.map { hint =>
      val hintOptions = hint.kvOptions.asScala.map { case (k, v) => k.toLowerCase -> v }
      val delay = hintOptions.getOrElse("delay", "0").toLong
      val frequency = hintOptions.getOrElse("frequency", "0").toLong
      (delay, frequency)
    }.getOrElse((0L, 0L)) // Default values if hint not present

    // create the StreamPhysicalIntervalJoin
    new StreamPhysicalIntervalJoin(
      join.getCluster,
      providedTraitSet,
      leftConversion(leftInput),
      rightConversion(rightInput),
      join.getJoinType,
      join.getCondition,
      remainCondition.getOrElse(join.getCluster.getRexBuilder.makeLiteral(true)),
      windowBounds.get,
      earlyFireDelay,
      earlyFireFrequency
    )
  }
}

object StreamPhysicalIntervalJoinRule {
  val INSTANCE: RelOptRule = new StreamPhysicalIntervalJoinRule
}
