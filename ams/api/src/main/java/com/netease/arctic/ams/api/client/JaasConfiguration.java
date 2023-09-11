/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netease.arctic.ams.api.client;

import java.util.HashMap;
import java.util.Map;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

/**
 * Copy from org.apache.hadoop.security.token.delegation.JaasConfiguration.
 */
public class JaasConfiguration extends Configuration {
  private final Configuration baseConfig = Configuration.getConfiguration();
  private static AppConfigurationEntry[] entry;
  private final String entryName;

  public JaasConfiguration(String entryName, String principal, String keytab) {
    this.entryName = entryName;
    Map<String, String> options = new HashMap<>();
    options.put("keyTab", keytab);
    options.put("principal", principal);
    options.put("useKeyTab", "true");
    options.put("storeKey", "true");
    options.put("useTicketCache", "false");
    options.put("refreshKrb5Config", "true");
    String jaasEnvVar = System.getenv("HADOOP_JAAS_DEBUG");
    if ("true".equalsIgnoreCase(jaasEnvVar)) {
      options.put("debug", "true");
    }

    entry = new AppConfigurationEntry[] {new AppConfigurationEntry(this.getKrb5LoginModuleName(),
        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
        options)};
  }

  public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
    return this.entryName.equals(name) ?
        entry :
        (this.baseConfig != null ? this.baseConfig.getAppConfigurationEntry(name) : null);
  }

  private String getKrb5LoginModuleName() {
    String krb5LoginModuleName;
    if (System.getProperty("java.vendor").contains("IBM")) {
      krb5LoginModuleName = "com.ibm.security.auth.module.Krb5LoginModule";
    } else {
      krb5LoginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
    }

    return krb5LoginModuleName;
  }
}
