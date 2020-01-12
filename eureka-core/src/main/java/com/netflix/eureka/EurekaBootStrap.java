/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.eureka;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.util.Date;

import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.appinfo.CloudInstanceConfig;
import com.netflix.appinfo.DataCenterInfo;
import com.netflix.appinfo.EurekaInstanceConfig;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.MyDataCenterInstanceConfig;
import com.netflix.appinfo.providers.EurekaConfigBasedInstanceInfoProvider;
import com.netflix.config.ConfigurationManager;
import com.netflix.config.DeploymentContext;
import com.netflix.discovery.DefaultEurekaClientConfig;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.EurekaClientConfig;
import com.netflix.discovery.converters.JsonXStream;
import com.netflix.discovery.converters.XmlXStream;
import com.netflix.eureka.aws.AwsBinder;
import com.netflix.eureka.aws.AwsBinderDelegate;
import com.netflix.eureka.cluster.PeerEurekaNodes;
import com.netflix.eureka.registry.AwsInstanceRegistry;
import com.netflix.eureka.registry.PeerAwareInstanceRegistry;
import com.netflix.eureka.registry.PeerAwareInstanceRegistryImpl;
import com.netflix.eureka.resources.DefaultServerCodecs;
import com.netflix.eureka.resources.ServerCodecs;
import com.netflix.eureka.util.EurekaMonitors;
import com.thoughtworks.xstream.XStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class that kick starts the eureka server.
 *
 * <p>
 * The eureka server is configured by using the configuration
 * {@link EurekaServerConfig} specified by <em>eureka.server.props</em> in the
 * classpath.  The eureka client component is also initialized by using the
 * configuration {@link EurekaInstanceConfig} specified by
 * <em>eureka.client.props</em>. If the server runs in the AWS cloud, the eureka
 * server binds it to the elastic ip as specified.
 * </p>
 *
 * @author Karthik Ranganathan, Greg Kim, David Liu\
 *
 * server端的启动类
EurekaBootStrap 实现了 javax.servlet.ServletContextListener 接口，
在 Servlet 容器( 例如 Tomcat、Jetty )启动时，调用 #contextInitialized() 方法，初始化 Eureka-Server


由于是Servlet应用，所以Eureka需要通过servlet的相关监听器 ServletContextListener 嵌入到 Servlet 的生命周期中。
EurekaBootStrap 类实现了该接口，在servlet标准的contextInitialized()方法中完成了初始化工作

 在当前的web.xml配置了监听器
<listener>
<listener-class>com.netflix.eureka.EurekaBootStrap</listener-class>
</listener>


1、StatusFilter：负责状态相关的处理逻辑；
2、ServerRequestAuthFilter:对请求进行授权认证处理；
3、RateLimitingFilter: 负责限流相关逻辑的，默认不开启，如果要打开eureka-server内置的限流功能，
需要把web文件中RateLimitingFilter的的注释打开，让这个filter生效。
-* GzipEncofingEnforcingFilter*: /v2/apps相关的请求，会走这里，仅仅对部分特殊的请求生效。

4、jersey：是核心filter，是个ServletContainer，是拦截所有请求的。

public class ServletContainer extends HttpServlet implements Filter {

}

5、welcome-file-list配置了status.jsp是欢迎页面，首页，eureka-server的控制台页面，展示注册服务的信息。
<welcome-file-list>
<welcome-file>jsp/status.jsp</welcome-file>
</welcome-file-list>
欢迎页

 */
public class EurekaBootStrap implements ServletContextListener {
    private static final Logger logger = LoggerFactory.getLogger(EurekaBootStrap.class);

    private static final String TEST = "test";

    private static final String ARCHAIUS_DEPLOYMENT_ENVIRONMENT = "archaius.deployment.environment";

    private static final String EUREKA_ENVIRONMENT = "eureka.environment";

    private static final String CLOUD = "cloud";
    private static final String DEFAULT = "default";

    private static final String ARCHAIUS_DEPLOYMENT_DATACENTER = "archaius.deployment.datacenter";

    private static final String EUREKA_DATACENTER = "eureka.datacenter";

    protected volatile EurekaServerContext serverContext;
    protected volatile AwsBinder awsBinder;

    private EurekaClient eurekaClient;

    /**
     * Construct a default instance of Eureka boostrap
     */
    public EurekaBootStrap() {
        this(null);
    }

    /**
     * Construct an instance of eureka bootstrap with the supplied eureka client
     *
     * @param eurekaClient the eureka client to bootstrap
     */
    public EurekaBootStrap(EurekaClient eurekaClient) {
        this.eurekaClient = eurekaClient;
    }

    /**
     * Initializes Eureka, including syncing up with other Eureka peers and publishing the registry.
     *
     * @see
     * javax.servlet.ServletContextListener#contextInitialized(javax.servlet.ServletContextEvent)
     */
    @Override
    public void contextInitialized(ServletContextEvent event) {//启动入口
        try {
            initEurekaEnvironment();
            initEurekaServerContext();

            ServletContext sc = event.getServletContext();
            sc.setAttribute(EurekaServerContext.class.getName(), serverContext);
        } catch (Throwable e) {
            logger.error("Cannot bootstrap eureka server :", e);
            throw new RuntimeException("Cannot bootstrap eureka server :", e);
        }
    }

    /**
     * Users can override to initialize the environment themselves.
     设置基于 Netflix Archaius 实现的配置文件的上下文，从而读取合适的配置文件。
     大多数情况下，只需要设置 EUREKA_ENVIRONMENT 即可，
     不同的服务器环境( 例如，PROD / TEST 等) 读取不同的配置文件
     */
    protected void initEurekaEnvironment() throws Exception {
        logger.info("Setting the eureka configuration..");

        String dataCenter = ConfigurationManager.getConfigInstance().getString(EUREKA_DATACENTER);
        if (dataCenter == null) {
            logger.info("Eureka data center value eureka.datacenter is not set, defaulting to default");
            ConfigurationManager.getConfigInstance().setProperty(ARCHAIUS_DEPLOYMENT_DATACENTER, DEFAULT);
        } else {
            ConfigurationManager.getConfigInstance().setProperty(ARCHAIUS_DEPLOYMENT_DATACENTER, dataCenter);
        }
        String environment = ConfigurationManager.getConfigInstance().getString(EUREKA_ENVIRONMENT);
        if (environment == null) {
            ConfigurationManager.getConfigInstance().setProperty(ARCHAIUS_DEPLOYMENT_ENVIRONMENT, TEST);
            logger.info("Eureka environment value eureka.environment is not set, defaulting to test");
        }
    }

    /**
     * init hook for server context. Override for custom logic.
    1、 初始化DefaultEurekaServerConfig: 加载了eureka-server.properties文件中的配置。
     配置信息的加载过程：
     1.1、DefaultEurekaServerConfig构造器中有个init()方法，它将所有配置加载到ConfigurationManager中去；
     1.2、DefaultEurekaServerConfig接口中有所有配置项的获取方法，
     通过get方式从DynamicPropertyFactory中获取配置，DynamicPropertyFactory是从ConfigurationManager衍生来的；
     1.3、在获取配置项的时候，如果没有配置，那么就会有默认的值，全部属性都是有默认值的。


     2、EurekaClient: 初始化eureka-server内部的一个eureka-client（用来跟其他的eureka-server节点进行注册和通信的）。
     此处需要注意EurekaInstanceConfig与EurekaClientConfig区别

     DiscoveryClient为EurekaClient的子类，它实现的主要功能：
     （1）读取EurekaClientConfig，包括TransportConfig
     （2）保存EurekaInstanceConfig和InstanceInfo
     （3）处理了是否要注册以及抓取注册表，如果不要的话，释放一些资源
     （4）支持调度的线程池
     （5）支持心跳的线程池
     （6）支持缓存刷新的线程池
     （7）EurekaTransport，支持底层的eureka client跟eureka server进行网络通信的组件，对网络通信组件进行了一些初始化的操作
     （8）抓取注册表

     3、构造了PeerAwareInstanceRegistry对象，用来感知eureka server集群的服务实例注册表；

     4、构造了PeerEurekaNodes对象，用于处理复制所有集群更新操作(注册、更新、取消、过期和状态更改) 。
     5、EurekaServerContext：完成eureka-server上下文（context）的构建。
     6、registry.syncUp()：从相邻的一个eureka server节点拷贝注册表的信息，如果拷贝失败，就找下一个
     7、EurekaMonitors.registerAllStats()： 监控机制，注册所有监测数据。

     */
    protected void initEurekaServerContext() throws Exception {
        EurekaServerConfig eurekaServerConfig = new DefaultEurekaServerConfig();//服务端的配置

        // For backward compatibility   Eureka-Server 请求和响应的数据兼容
        //目前 Eureka-Server 提供 V2 版本 API ，如上代码主要对 V1 版本 API 做兼容。可以选择跳过。
        JsonXStream.getInstance().registerConverter(new V1AwareInstanceInfoConverter(), XStream.PRIORITY_VERY_HIGH);
        XmlXStream.getInstance().registerConverter(new V1AwareInstanceInfoConverter(), XStream.PRIORITY_VERY_HIGH);

        logger.info("Initializing the eureka client...");
        logger.info(eurekaServerConfig.getJsonCodecName());//创建 Eureka-Server 请求和响应编解码器
        ServerCodecs serverCodecs = new DefaultServerCodecs(eurekaServerConfig);

        ApplicationInfoManager applicationInfoManager = null;

        //创建 Eureka-Client
        //Eureka-Server 内嵌 Eureka-Client，用于和 Eureka-Server 集群里其他节点通信交互。
        //Eureka-Client 也可以通过 EurekaBootStrap 构造方法传递[大多数情况下用不到。]
        if (eurekaClient == null) {
            EurekaInstanceConfig instanceConfig = isCloud(ConfigurationManager.getDeploymentContext())
                    ? new CloudInstanceConfig()
                    : new MyDataCenterInstanceConfig();//是云配置还是本地配置
            //根据EurekaInstanceConfig生成一个applicationInfoManager对象
            applicationInfoManager = new ApplicationInfoManager(
                    instanceConfig, new EurekaConfigBasedInstanceInfoProvider(instanceConfig).get());

            EurekaClientConfig eurekaClientConfig = new DefaultEurekaClientConfig();
            eurekaClient = new DiscoveryClient(applicationInfoManager, eurekaClientConfig);//根据applicationInfoManager和eurekaClientConfig生成eurekaClient
        } else {
            applicationInfoManager = eurekaClient.getApplicationInfoManager();
        }

        PeerAwareInstanceRegistry registry;
        if (isAws(applicationInfoManager.getInfo())) {
            registry = new AwsInstanceRegistry(
                    eurekaServerConfig,
                    eurekaClient.getEurekaClientConfig(),
                    serverCodecs,
                    eurekaClient
            );
            awsBinder = new AwsBinderDelegate(eurekaServerConfig, eurekaClient.getEurekaClientConfig(), registry, applicationInfoManager);
            awsBinder.start();
        } else {//创建应用实例信息的注册表
            registry = new PeerAwareInstanceRegistryImpl(
                    eurekaServerConfig,
                    eurekaClient.getEurekaClientConfig(),
                    serverCodecs,
                    eurekaClient
            );
        }
        //创建 Eureka-Server 集群节点集合
        PeerEurekaNodes peerEurekaNodes = getPeerEurekaNodes(
                registry,
                eurekaServerConfig,
                eurekaClient.getEurekaClientConfig(),
                serverCodecs,
                applicationInfoManager
        );
        //创建 Eureka-Server 上下文
        serverContext = new DefaultEurekaServerContext(
                eurekaServerConfig,
                serverCodecs,
                registry,
                peerEurekaNodes,
                applicationInfoManager
        );

        EurekaServerContextHolder.initialize(serverContext);

        serverContext.initialize();
        logger.info("Initialized server context");

        /**
         若调用 #syncUp() 方法，未获取到应用实例，则 Eureka-Server 会有一段时间( 默认：5 分钟，可配 )
         不允许被 Eureka-Client 获取注册信息，避免影响 Eureka-Client 。
         */
        // Copy registry from neighboring eureka node
        int registryCount = registry.syncUp();//从其他 Eureka-Server 拉取注册信息
        registry.openForTraffic(applicationInfoManager, registryCount);

        // Register all monitoring statistics.
        EurekaMonitors.registerAllStats();//注册监控
    }

    protected PeerEurekaNodes getPeerEurekaNodes(PeerAwareInstanceRegistry registry, EurekaServerConfig eurekaServerConfig, EurekaClientConfig eurekaClientConfig, ServerCodecs serverCodecs, ApplicationInfoManager applicationInfoManager) {
        PeerEurekaNodes peerEurekaNodes = new PeerEurekaNodes(
                registry,
                eurekaServerConfig,
                eurekaClientConfig,
                serverCodecs,
                applicationInfoManager
        );

        return peerEurekaNodes;
    }

    /**
     * Handles Eureka cleanup, including shutting down all monitors and yielding all EIPs.
     *
     * @see javax.servlet.ServletContextListener#contextDestroyed(javax.servlet.ServletContextEvent)
     * 监听器的ServletContextListener用来销毁servlet对象
     */
    @Override
    public void contextDestroyed(ServletContextEvent event) {
        try {
            logger.info("{} Shutting down Eureka Server..", new Date());
            ServletContext sc = event.getServletContext();
            sc.removeAttribute(EurekaServerContext.class.getName());

            destroyEurekaServerContext();
            destroyEurekaEnvironment();

        } catch (Throwable e) {
            logger.error("Error shutting down eureka", e);
        }
        logger.info("{} Eureka Service is now shutdown...", new Date());
    }

    /**
     * Server context shutdown hook. Override for custom logic
     */
    protected void destroyEurekaServerContext() throws Exception {
        EurekaMonitors.shutdown();
        if (awsBinder != null) {
            awsBinder.shutdown();
        }
        if (serverContext != null) {
            serverContext.shutdown();
        }
    }

    /**
     * Users can override to clean up the environment themselves.
     */
    protected void destroyEurekaEnvironment() throws Exception {

    }
    //是否是aws
    protected boolean isAws(InstanceInfo selfInstanceInfo) {
        boolean result = DataCenterInfo.Name.Amazon == selfInstanceInfo.getDataCenterInfo().getName();
        logger.info("isAws returned {}", result);
        return result;
    }
    //是否开启云配置
    protected boolean isCloud(DeploymentContext deploymentContext) {
        logger.info("Deployment datacenter is {}", deploymentContext.getDeploymentDatacenter());
        return CLOUD.equals(deploymentContext.getDeploymentDatacenter());
    }
}
