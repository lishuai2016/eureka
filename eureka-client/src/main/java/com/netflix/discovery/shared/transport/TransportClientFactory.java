package com.netflix.discovery.shared.transport;

import com.netflix.discovery.shared.resolver.EurekaEndpoint;

/**
 * A low level client factory interface. Not advised to be used by top level consumers.
 *
 * @author David Liu
 * 创建 EurekaHttpClient 的工厂接口
 * 大多数 EurekaHttpClient 实现类都有其对应的工厂实现类。
 */
public interface TransportClientFactory {

    EurekaHttpClient newClient(EurekaEndpoint serviceUrl);

    void shutdown();

}
