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

package org.apache.flink.table.planner.hint;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/** Configuration options for early firing in joins. */
@PublicEvolving
public class EarlyFireHintOptions {
    public static final String FIRE_INTERVAL = "fire-interval";
    public static final String MAX_LATENESS = "max-lateness";

    public static final ConfigOption<Duration> EARLY_FIRE_INTERVAL =
            ConfigOptions.key(FIRE_INTERVAL)
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription(
                            "The interval at which to fire early results in interval/window joins.");

    public static final ConfigOption<Duration> EARLY_FIRE_MAX_LATENESS =
            ConfigOptions.key(MAX_LATENESS)
                    .durationType()
                    .defaultValue(Duration.ofMinutes(10))
                    .withDescription(
                            "The maximum lateness allowed for early firing in interval/window joins.");
}
