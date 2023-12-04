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

import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;

import java.util.Collections;
import java.util.List;

public class AmoroGaugeMetricFamily extends GaugeMetricFamily {

  private final List<String> labelNames;

  public AmoroGaugeMetricFamily(String name, String help, double value) {
    super(name, help, value);
    this.labelNames = Collections.emptyList();
  }

  public AmoroGaugeMetricFamily(String name, String help, List<String> labelNames) {
    super(name, help, labelNames);
    this.labelNames = labelNames;
  }

  public GaugeMetricFamily addMetric(List<String> labelValues, double value) {
    if (labelValues.size() != this.labelNames.size()) {
      throw new IllegalArgumentException("Incorrect number of labels.");
    } else {
      this.samples.add(new Collector.MetricFamilySamples.Sample(this.name, this.labelNames, labelValues, value,
          System.currentTimeMillis()));
      return this;
    }
  }

}
