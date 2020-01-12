package com.netflix.eureka.cluster;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.EurekaClientConfig;
import com.netflix.discovery.endpoint.EndpointUtils;
import com.netflix.eureka.EurekaServerConfig;
import com.netflix.eureka.registry.PeerAwareInstanceRegistry;
import com.netflix.eureka.resources.ServerCodecs;
import com.netflix.eureka.transport.JerseyReplicationClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to manage lifecycle of a collection of {@link PeerEurekaNode}s.
 *
 * @author Tomasz Bak
 */
@Singleton
public class PeerEurekaNodes {

    private static final Logger logger = LoggerFactory.getLogger(PeerEurekaNodes.class);

    protected final PeerAwareInstanceRegistry registry;//应用实例注册表
    protected final EurekaServerConfig serverConfig;//Eureka-Server 配置
    protected final EurekaClientConfig clientConfig;//Eureka-Client 配置
    protected final ServerCodecs serverCodecs;// Eureka-Server 编解码
    private final ApplicationInfoManager applicationInfoManager;//应用实例信息管理器

    private volatile List<PeerEurekaNode> peerEurekaNodes = Collections.emptyList();//存放集群节点信息
    private volatile Set<String> peerEurekaNodeUrls = Collections.emptySet();//存放集群节点的URL列表

    private ScheduledExecutorService taskExecutor;

    @Inject
    public PeerEurekaNodes(
            PeerAwareInstanceRegistry registry,
            EurekaServerConfig serverConfig,
            EurekaClientConfig clientConfig,
            ServerCodecs serverCodecs,
            ApplicationInfoManager applicationInfoManager) {
        this.registry = registry;
        this.serverConfig = serverConfig;
        this.clientConfig = clientConfig;
        this.serverCodecs = serverCodecs;
        this.applicationInfoManager = applicationInfoManager;
    }

    public List<PeerEurekaNode> getPeerNodesView() {//提供一个集群节点只读视图
        return Collections.unmodifiableList(peerEurekaNodes);
    }

    public List<PeerEurekaNode> getPeerEurekaNodes() {
        return peerEurekaNodes;
    }

    public int getMinNumberOfAvailablePeers() {
        return serverConfig.getHealthStatusMinNumberOfAvailablePeers();
    }

    /**
     *
     peerEurekaNodes, peerEurekaNodeUrls, taskExecutor 属性，在构造方法中未设置和初始化，
     而是在 PeerEurekaNodes#start() 方法，设置和初始化

     peerEurekaNodes.start()主要是启动一个只拥有一个线程的线程池，第一次进去会更新一下集群其他节点信息
     然后启动了一个定时线程，每60秒更新一次，也就是说后续可以根据配置动态的修改节点配置。
     */
    public void start() {
        taskExecutor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r, "Eureka-PeerNodesUpdater");
                        thread.setDaemon(true);
                        return thread;
                    }
                }
        );
        try {// 初始化 集群节点信息
            updatePeerEurekaNodes(resolvePeerUrls());// 首次进来，更新集群节点信息
            Runnable peersUpdateTask = new Runnable() { // 初始化 初始化固定周期更新集群节点信息的任务
                @Override
                public void run() {
                    try {
                        updatePeerEurekaNodes(resolvePeerUrls());
                    } catch (Throwable e) {
                        logger.error("Cannot update the replica Nodes", e);
                    }

                }
            };
            taskExecutor.scheduleWithFixedDelay(//把任务丢到线程池中去执行
                    peersUpdateTask,
                    serverConfig.getPeerEurekaNodesUpdateIntervalMs(),
                    serverConfig.getPeerEurekaNodesUpdateIntervalMs(),
                    TimeUnit.MILLISECONDS
            );
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        for (PeerEurekaNode node : peerEurekaNodes) {// 打印 集群节点信息
            logger.info("Replica node URL:  {}", node.getServiceUrl());
        }
    }

    public void shutdown() {
        taskExecutor.shutdown();//关闭线程池
        List<PeerEurekaNode> toRemove = this.peerEurekaNodes;

        this.peerEurekaNodes = Collections.emptyList();//清空集合是否内存
        this.peerEurekaNodeUrls = Collections.emptySet();

        for (PeerEurekaNode node : toRemove) {
            node.shutDown();
        }
    }

    /**
     * Resolve peer URLs.
     *
     * @return peer URLs with node's own URL filtered out
     */
    protected List<String> resolvePeerUrls() {// 获得 Eureka-Server 集群服务地址数组
        InstanceInfo myInfo = applicationInfoManager.getInfo();
        String zone = InstanceInfo.getZone(clientConfig.getAvailabilityZones(clientConfig.getRegion()), myInfo);
        List<String> replicaUrls = EndpointUtils
                .getDiscoveryServiceUrls(clientConfig, zone, new EndpointUtils.InstanceInfoBasedUrlRandomizer(myInfo));

        int idx = 0;
        while (idx < replicaUrls.size()) {
            if (isThisMyUrl(replicaUrls.get(idx))) {//移除自己的URL
                replicaUrls.remove(idx);
            } else {
                idx++;
            }
        }
        return replicaUrls;
    }

    /**
     * Given new set of replica URLs, destroy {@link PeerEurekaNode}s no longer available, and
     * create new ones.
     *
     * @param newPeerUrls peer node URLs; this collection should have local node's URL filtered out
     */
    protected void updatePeerEurekaNodes(List<String> newPeerUrls) {
        if (newPeerUrls.isEmpty()) {
            logger.warn("The replica size seems to be empty. Check the route 53 DNS Registry");
            return;
        }

        Set<String> toShutdown = new HashSet<>(peerEurekaNodeUrls);
        toShutdown.removeAll(newPeerUrls);//计算以前有现在没有的集合，即要执行删除操作的
        Set<String> toAdd = new HashSet<>(newPeerUrls);
        toAdd.removeAll(peerEurekaNodeUrls);//计算这一次有，而上一次没有的集合，即新增的

        if (toShutdown.isEmpty() && toAdd.isEmpty()) { // No change
            return;
        }

        // Remove peers no long available  拷贝一份节点集合，删除不可用URL对应的节点
        List<PeerEurekaNode> newNodeList = new ArrayList<>(peerEurekaNodes);

        if (!toShutdown.isEmpty()) {
            logger.info("Removing no longer available peer nodes {}", toShutdown);
            int i = 0;
            while (i < newNodeList.size()) {
                PeerEurekaNode eurekaNode = newNodeList.get(i);
                if (toShutdown.contains(eurekaNode.getServiceUrl())) {
                    newNodeList.remove(i);
                    eurekaNode.shutDown();//移除的节点进行关闭
                } else {
                    i++;
                }
            }
        }

        // Add new peers
        if (!toAdd.isEmpty()) {
            logger.info("Adding new peer nodes {}", toAdd);
            for (String peerUrl : toAdd) {
                newNodeList.add(createPeerEurekaNode(peerUrl));//根据URL生成节点
            }
        }

        this.peerEurekaNodes = newNodeList;//更新最新的节点集合
        this.peerEurekaNodeUrls = new HashSet<>(newPeerUrls);//更新最新的节点URL集合
    }

    protected PeerEurekaNode createPeerEurekaNode(String peerEurekaNodeUrl) {
        //创建 Eureka-Server 集群通信客户端
        HttpReplicationClient replicationClient = JerseyReplicationClient.createReplicationClient(serverConfig, serverCodecs, peerEurekaNodeUrl);
        String targetHost = hostFromUrl(peerEurekaNodeUrl);
        if (targetHost == null) {
            targetHost = "host";
        }
        return new PeerEurekaNode(registry, targetHost, peerEurekaNodeUrl, replicationClient, serverConfig);
    }

    /**
     * @deprecated 2016-06-27 use instance version of {@link #isThisMyUrl(String)}
     *
     * Checks if the given service url contains the current host which is trying
     * to replicate. Only after the EIP binding is done the host has a chance to
     * identify itself in the list of replica nodes and needs to take itself out
     * of replication traffic.
     *
     * @param url the service url of the replica node that the check is made.
     * @return true, if the url represents the current node which is trying to
     *         replicate, false otherwise.
     */
    public static boolean isThisMe(String url) {
        InstanceInfo myInfo = ApplicationInfoManager.getInstance().getInfo();
        String hostName = hostFromUrl(url);
        return hostName != null && hostName.equals(myInfo.getHostName());
    }

    /**
     * Checks if the given service url contains the current host which is trying
     * to replicate. Only after the EIP binding is done the host has a chance to
     * identify itself in the list of replica nodes and needs to take itself out
     * of replication traffic.
     *
     * @param url the service url of the replica node that the check is made.
     * @return true, if the url represents the current node which is trying to
     *         replicate, false otherwise.
     */
    public boolean isThisMyUrl(String url) {//判断当前URL是不是服务自身的地址
        final String myUrlConfigured = serverConfig.getMyUrl();
        if (myUrlConfigured != null) {
            return myUrlConfigured.equals(url);
        }
        return isInstanceURL(url, applicationInfoManager.getInfo());
    }

    /**
     * Checks if the given service url matches the supplied instance
     *
     * @param url the service url of the replica node that the check is made.
     * @param instance the instance to check the service url against
     * @return true, if the url represents the supplied instance, false otherwise.
     */
    public boolean isInstanceURL(String url, InstanceInfo instance) {
        String hostName = hostFromUrl(url);
        String myInfoComparator = instance.getHostName();
        if (clientConfig.getTransportConfig().applicationsResolverUseIp()) {
            myInfoComparator = instance.getIPAddr();
        }
        return hostName != null && hostName.equals(myInfoComparator);
    }

    public static String hostFromUrl(String url) {
        URI uri;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            logger.warn("Cannot parse service URI {}", url, e);
            return null;
        }
        return uri.getHost();
    }
}
