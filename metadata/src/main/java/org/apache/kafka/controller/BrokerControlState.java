/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.controller;


public enum BrokerControlState {
    // broker同步leader的元信息数据落后了
    FENCED(true, false),
    // broker同步leader的元信息数据正常，没有落后
    UNFENCED(false, false),
    //
    CONTROLLED_SHUTDOWN(false, false),
    // broker关闭了
    SHUTDOWN_NOW(true, true);

    private final boolean fenced;
    private final boolean shouldShutDown;

    BrokerControlState(boolean fenced, boolean shouldShutDown) {
        this.fenced = fenced;
        this.shouldShutDown = shouldShutDown;
    }

    public boolean fenced() {
        return fenced;
    }

    public boolean shouldShutDown() {
        return shouldShutDown;
    }

    public boolean inControlledShutdown() {
        return this == CONTROLLED_SHUTDOWN;
    }
}
