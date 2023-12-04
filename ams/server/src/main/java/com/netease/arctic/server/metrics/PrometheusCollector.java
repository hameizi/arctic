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

import com.netease.arctic.ams.api.metrics.MetricsContent;
import com.netease.arctic.ams.api.metrics.TaggedMetrics;
import io.prometheus.client.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PrometheusCollector extends Collector {

  Map<String, MetricFamilySamples> mfs = new ConcurrentHashMap<>();

  public void addMetric(MetricsContent<?> metrics) {
    TaggedMetrics taggedMetrics = TaggedMetrics.from(metrics);
    List<String> labelNames = new ArrayList<>();
    List<String> labelValues = new ArrayList<>();
    taggedMetrics.tags().forEach((k, v) -> {
      labelNames.add(k);
      labelValues.add(v.toString());
    });
    taggedMetrics.metrics().forEach((k, v) -> {
      mfs.putIfAbsent(metrics.name() + k, new AmoroGaugeMetricFamily(metrics.name(), "",
          labelNames));
      MetricFamilySamples familySamples = mfs.get(metrics.name() + k);
      ((AmoroGaugeMetricFamily) familySamples).addMetric(labelValues, Double.valueOf(v.toString()));
      System.out.println("addMetric " + metrics.name() + " labelNames " + String.join(",", labelNames) + " " +
          "labelValues " + String.join(",", labelValues) + " value " + v + " time " + System.currentTimeMillis());
    });
  }

  @Override
  public List<MetricFamilySamples> collect() {
    List<MetricFamilySamples> result = new ArrayList<>(mfs.values());
    System.out.println("collect" + result.size());
    mfs = new ConcurrentHashMap<>();
    return result;
  }
}
