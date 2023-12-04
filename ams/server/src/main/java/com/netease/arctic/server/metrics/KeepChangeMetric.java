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

public class KeepChangeMetric  implements MetricsContent<KeepChangeMetric> {

  private String tableName = "test_KeepChange_table";
  private Integer metric;

  public void setMetric(Integer metric) {
    this.metric = metric;
  }

  public static KeepChangeMetric create(Integer metric) {
    KeepChangeMetric keepChangeMetric = new KeepChangeMetric();
    keepChangeMetric.setMetric(metric);
    return keepChangeMetric;
  }

  @TaggedMetrics.Tag(name = "table_name")
  public String tableName() {
    return tableName;
  }

  @TaggedMetrics.Metric(name = "keepmetric")
  public Integer metric() {
    return metric;
  }

  @Override
  public String name() {
    return "KeepChangeMetric";
  }

  @Override
  public MetricType type() {
    return MetricType.SERVICE;
  }

  @Override
  public KeepChangeMetric data() {
    return this;
  }
}
