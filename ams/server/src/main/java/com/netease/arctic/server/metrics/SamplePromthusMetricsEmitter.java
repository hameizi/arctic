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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Maps;
import com.netease.arctic.ams.api.metrics.MetricType;
import com.netease.arctic.ams.api.metrics.MetricsContent;
import com.netease.arctic.ams.api.metrics.MetricsEmitter;
import com.netease.arctic.ams.api.metrics.TaggedMetrics;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.HTTPServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

public class SamplePromthusMetricsEmitter implements MetricsEmitter {

  PrometheusCollector collector;

  public static void main(String[] args) {
    SamplePromthusMetricsEmitter promthusMetricsEmitter = new SamplePromthusMetricsEmitter();
    promthusMetricsEmitter.open(Maps.newHashMap());
    new Thread(() -> {
      int i = 0;
      while (true) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        i++;
        if (i % 10 == 0) {
          promthusMetricsEmitter.emit(EventMetric.create(i));
        }
      }
    }).start();

    new Thread(() -> {
      int i = 0;
      while (true) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        if (i > 20) {
          break;
        }
        promthusMetricsEmitter.emit(KeepChangeMetric.create(i++));
      }
    }).start();

    new Thread(() -> {
      int i = 0;
      while (true) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        i++;
        if (i % 10 == 0) {
          promthusMetricsEmitter.emit(PlanDurationMetric.create("running", i, 1));
        }
        if (i % 20 == 0) {
          promthusMetricsEmitter.emit(PlanDurationMetric.create("pending", i, 1));
        }
        if (i % 30 == 0) {
          promthusMetricsEmitter.emit(PlanDurationMetric.create("commit", i, 1));
        }
      }
    }).start();
  }

  @Override
  public void open(Map<String, String> properties) {
    collector = new PrometheusCollector();
    collector.register();

    try {
      HTTPServer httpServer = new HTTPServer(8002);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {

  }

  @Override
  public String name() {
    return "Prometheus";
  }

  @Override
  public void emit(MetricsContent<?> metrics) {
    collector.addMetric(metrics);
  }

  @Override
  public boolean accept(MetricsContent<?> metrics) {
    return metrics.type() == MetricType.SERVICE;
  }
}
