/*
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.discovery.shared.resolver;

/**
 EndPoint ，服务端点。例如，Eureka-Server 的访问地址。
 EndPoint 解析器，将配置的 Eureka-Server 的访问地址解析成 EndPoint 。
 */

public interface EurekaEndpoint extends Comparable<Object> {

    String getServiceUrl();

    /**
     * @deprecated use {@link #getNetworkAddress()}
     */
    @Deprecated
    String getHostName();

    String getNetworkAddress();

    int getPort();

    boolean isSecure();

    String getRelativeUri();

}
