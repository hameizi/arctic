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

package com.netease.arctic.server.metrics;

import com.netease.arctic.ams.api.metrics.MetricType;
import com.netease.arctic.ams.api.metrics.MetricsContent;
import com.netease.arctic.ams.api.metrics.TaggedMetrics;

public class PlanDurationMetric implements MetricsContent<PlanDurationMetric> {

  private String tableName = "test_plan_table";

  private String optimizingStatus;
  private Integer planDuration;
  private Integer statue;

  public PlanDurationMetric(String optimizingStatus, Integer planDuration, Integer statue) {
    this.optimizingStatus = optimizingStatus;
    this.planDuration = planDuration;
    this.statue = statue;
  }

  public static PlanDurationMetric create(String optimizingStatus, Integer planDuration, Integer statue) {
    return new PlanDurationMetric(optimizingStatus, planDuration, statue);
  }

  public void setOptimizingStatus(String optimizingStatus) {
    this.optimizingStatus = optimizingStatus;
  }

  public void setPlanDuration(Integer planDuration) {
    this.planDuration = planDuration;
  }

  public void setStatue(Integer statue) {
    this.statue = statue;
  }

  @TaggedMetrics.Tag(name = "table_name")
  public String tableName() {
    return tableName;
  }

  @TaggedMetrics.Tag(name = "table_name")
  public String optimizingStatus() {
    return optimizingStatus;
  }

  @TaggedMetrics.Metric(name = "planDuration")
  public Integer planDuration() {
    return planDuration;
  }

  @TaggedMetrics.Metric(name = "statue")
  public Integer statueDuration() {
    return statue;
  }


  @Override
  public String name() {
    return "plan_duration";
  }

  @Override
  public MetricType type() {
    return MetricType.SERVICE;
  }

  @Override
  public PlanDurationMetric data() {
    return this;
  }
}
