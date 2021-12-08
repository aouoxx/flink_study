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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.clusterframework.TaskExecutorResourceSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorResourceUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.entrypoint.ClusterConfiguration;
import org.apache.flink.runtime.entrypoint.ClusterConfigurationParserFactory;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTrackerImpl;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalException;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.util.MetricUtils;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.taskmanager.MemoryLogger;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is the executable entry point for the task manager in yarn or standalone mode.
 * It constructs the related components (network, I/O manager, memory manager, RPC service, HA service)
 * and starts them.
 */
public class TaskManagerRunner implements FatalErrorHandler, AutoCloseableAsync {

	private static final Logger LOG = LoggerFactory.getLogger(TaskManagerRunner.class);

	private static final long FATAL_ERROR_SHUTDOWN_TIMEOUT_MS = 10000L;

	private static final int STARTUP_FAILURE_RETURN_CODE = 1;

	public static final int RUNTIME_FAILURE_RETURN_CODE = 2;

	private final Object lock = new Object();

	private final Configuration configuration;

	private final ResourceID resourceId;

	private final Time timeout;

	private final RpcService rpcService;

	private final HighAvailabilityServices highAvailabilityServices;

	private final MetricRegistryImpl metricRegistry;

	private final BlobCacheService blobCacheService;

	/** Executor used to run future callbacks. */
	private final ExecutorService executor;

	private final TaskExecutor taskManager;

	private final CompletableFuture<Void> terminationFuture;

	private boolean shutdown;

	public TaskManagerRunner(Configuration configuration, ResourceID resourceId) throws Exception {
		this.configuration = checkNotNull(configuration);
		this.resourceId = checkNotNull(resourceId);
		// todo 获取akka的超时时间
		timeout = AkkaUtils.getTimeoutAsTime(configuration);
		// todo 创建一个taskManager 的线程池, corePoolSize 和机器CPU数量一致
		this.executor = java.util.concurrent.Executors.newScheduledThreadPool(
			Hardware.getNumberCPUCores(),
			new ExecutorThreadFactory("taskmanager-future"));

		// todo 创建高可用服务, 负责选举JobManager 和 ResourceManger 获取leader 信息
		highAvailabilityServices = HighAvailabilityServicesUtils.createHighAvailabilityServices(
			configuration,
			executor,
			HighAvailabilityServicesUtils.AddressResolution.TRY_ADDRESS_RESOLUTION);

		// todo 创建RPC 服务, 和其他Flink 进程互相通信的时候使用, 用于连接一个RpcEndpoint 连接成功之后返回一个RPCGateWay
		rpcService = createRpcService(configuration, highAvailabilityServices);

		// todo 创建心跳服务
		HeartbeatServices heartbeatServices = HeartbeatServices.fromConfiguration(configuration);

		// todo 创建监控指标注册, 用于记录所有的metric, 连接metricGroup 和MetricReporter
		metricRegistry = new MetricRegistryImpl(
			MetricRegistryConfiguration.fromConfiguration(configuration),
			ReporterSetup.fromConfiguration(configuration));

		// todo 开启metrics 查询服务, 以key-value 方式Flink 中已注册的metrics
		final RpcService metricQueryServiceRpcService = MetricUtils.startMetricsRpcService(configuration, rpcService.getAddress());
		metricRegistry.startQueryService(metricQueryServiceRpcService, resourceId);

		// todo 创建BlobCache 服务
		blobCacheService = new BlobCacheService(
			configuration, highAvailabilityServices.createBlobStore(), null
		);

		// todo 启动TaskManager
		taskManager = startTaskManager(
			this.configuration,
			this.resourceId,
			rpcService,
			highAvailabilityServices,
			heartbeatServices,
			metricRegistry,
			blobCacheService,
			false,
			this);

		this.terminationFuture = new CompletableFuture<>();
		this.shutdown = false;

		MemoryLogger.startIfConfigured(LOG, configuration, terminationFuture);
	}

	// --------------------------------------------------------------------------------------------
	//  Lifecycle management
	// --------------------------------------------------------------------------------------------

	/**
	 * 通过RPC服务启动TaskExecutor, 找它的onStart的方法
	 * @throws Exception
	 */
	public void start() throws Exception {
		taskManager.start();
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		synchronized (lock) {
			if (!shutdown) {
				shutdown = true;

				final CompletableFuture<Void> taskManagerTerminationFuture = taskManager.closeAsync();

				final CompletableFuture<Void> serviceTerminationFuture = FutureUtils.composeAfterwards(
					taskManagerTerminationFuture,
					this::shutDownServices);

				serviceTerminationFuture.whenComplete(
					(Void ignored, Throwable throwable) -> {
						if (throwable != null) {
							terminationFuture.completeExceptionally(throwable);
						} else {
							terminationFuture.complete(null);
						}
					});
			}
		}

		return terminationFuture;
	}

	private CompletableFuture<Void> shutDownServices() {
		synchronized (lock) {
			Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);
			Exception exception = null;

			try {
				blobCacheService.close();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			try {
				metricRegistry.shutdown();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			try {
				highAvailabilityServices.close();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			terminationFutures.add(rpcService.stopService());

			terminationFutures.add(ExecutorUtils.nonBlockingShutdown(timeout.toMilliseconds(), TimeUnit.MILLISECONDS, executor));

			if (exception != null) {
				terminationFutures.add(FutureUtils.completedExceptionally(exception));
			}

			return FutureUtils.completeAll(terminationFutures);
		}
	}

	// export the termination future for caller to know it is terminated
	public CompletableFuture<Void> getTerminationFuture() {
		return terminationFuture;
	}

	// --------------------------------------------------------------------------------------------
	//  FatalErrorHandler methods
	// --------------------------------------------------------------------------------------------

	@Override
	public void onFatalError(Throwable exception) {
		LOG.error("Fatal error occurred while executing the TaskManager. Shutting it down...", exception);

		if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(exception)) {
			terminateJVM();
		} else {
			closeAsync();

			FutureUtils.orTimeout(terminationFuture, FATAL_ERROR_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);

			terminationFuture.whenComplete(
				(Void ignored, Throwable throwable) -> {
					terminateJVM();
				});
		}
	}

	private void terminateJVM() {
		System.exit(RUNTIME_FAILURE_RETURN_CODE);
	}

	// --------------------------------------------------------------------------------------------
	//  Static entry point
	// --------------------------------------------------------------------------------------------

	public static void main(String[] args) throws Exception {
		// startup checks and logging todo 日志打印环境
		EnvironmentInformation.logEnvironmentInfo(LOG, "TaskManager", args);
		// todo 注册信号处理句柄, 在linux下响应TERM, HUP, INT
		SignalHandler.register(LOG);
		// todo 增加shutdownlook 在JVM关闭之前回调, 让JVM关闭延迟5s
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		// todo 获取并打印最大打开文件句柄限制
		long maxOpenFileHandles = EnvironmentInformation.getOpenFileHandlesLimit();

		if (maxOpenFileHandles != -1L) {
			LOG.info("Maximum number of open file descriptors is {}.", maxOpenFileHandles);
		} else {
			LOG.info("Cannot determine the maximum number of open file descriptors");
		}
		// todo 启动taskManager
		runTaskManagerSecurely(args, ResourceID.generate());
	}

	public static Configuration loadConfiguration(String[] args) throws FlinkParseException {
		final CommandLineParser<ClusterConfiguration> commandLineParser = new CommandLineParser<>(new ClusterConfigurationParserFactory());

		final ClusterConfiguration clusterConfiguration;

		try {
			clusterConfiguration = commandLineParser.parse(args);
		} catch (FlinkParseException e) {
			LOG.error("Could not parse the command line options.", e);
			commandLineParser.printHelp(TaskManagerRunner.class.getSimpleName());
			throw e;
		}

		final Configuration dynamicProperties = ConfigurationUtils.createConfiguration(clusterConfiguration.getDynamicProperties());
		return GlobalConfiguration.loadConfiguration(clusterConfiguration.getConfigDir(), dynamicProperties);
	}

	/**
	 * TODO 启动TaskManagerRunner
	 * @param configuration
	 * @param resourceId
	 * @throws Exception
	 */
	public static void runTaskManager(Configuration configuration, ResourceID resourceId) throws Exception {
		// todo 创建一个TaskManagerRunner 使用随机的ResourceId
		final TaskManagerRunner taskManagerRunner = new TaskManagerRunner(configuration, resourceId);
		// todo 调用start
		taskManagerRunner.start();
	}

	public static void runTaskManagerSecurely(String[] args, ResourceID resourceID) {
		try {
			// todo 读取flink-conf.yaml 和命令行传入的动态参数 作为配置信息
			final Configuration configuration = loadConfiguration(args);
			// todo 初始化共享文件系统配置 (FileSystemFactory) 比如HDFS
			FileSystem.initialize(configuration, PluginUtils.createPluginManagerFromRootFolder(configuration));

			// todo 读取安全相关配置
			SecurityUtils.install(new SecurityConfiguration(configuration));
			// todo 以安全认证环境下调用runTaskManager
			SecurityUtils.getInstalledContext().runSecured(() -> {
				runTaskManager(configuration, resourceID);
				return null;
			});
		} catch (Throwable t) {
			final Throwable strippedThrowable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
			LOG.error("TaskManager initialization failed.", strippedThrowable);
			System.exit(STARTUP_FAILURE_RETURN_CODE);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Static utilities
	// --------------------------------------------------------------------------------------------

	public static TaskExecutor startTaskManager(
			Configuration configuration,
			ResourceID resourceID,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			BlobCacheService blobCacheService,
			boolean localCommunicationOnly,
			FatalErrorHandler fatalErrorHandler) throws Exception {

		checkNotNull(configuration);
		checkNotNull(resourceID);
		checkNotNull(rpcService);
		checkNotNull(highAvailabilityServices);

		LOG.info("Starting TaskManager with ResourceID: {}", resourceID);

		// todo 获取外部访问地址
		InetAddress remoteAddress = InetAddress.getByName(rpcService.getAddress());

		// todo 获取task executor资源详情
		// todo 包括CPU核数, task堆内存, task堆外内存 和 managed 内存
		final TaskExecutorResourceSpec taskExecutorResourceSpec;
		taskExecutorResourceSpec = TaskExecutorResourceUtils.resourceSpecFromConfig(configuration);

		// todo 获取taskmanager服务配置
		TaskManagerServicesConfiguration taskManagerServicesConfiguration =
			TaskManagerServicesConfiguration.fromConfiguration(
				configuration,
				resourceID,
				remoteAddress,
				localCommunicationOnly,
				taskExecutorResourceSpec);

		// todo 创建task manager 的Metric Group
		Tuple2<TaskManagerMetricGroup, MetricGroup> taskManagerMetricGroup = MetricUtils.instantiateTaskManagerMetricGroup(
			metricRegistry,
			TaskManagerLocation.getHostName(remoteAddress),
			resourceID,
			taskManagerServicesConfiguration.getSystemResourceMetricsProbingInterval());

		// todo 创建task manager 服务, 是其他多种资源或服务的容器
		TaskManagerServices taskManagerServices = TaskManagerServices.fromConfiguration(
			taskManagerServicesConfiguration,
			taskManagerMetricGroup.f1,
			rpcService.getExecutor()); // TODO replace this later with some dedicated executor for io.

		// todo 创建task manager的配置
		TaskManagerConfiguration taskManagerConfiguration =
			TaskManagerConfiguration.fromConfiguration(configuration, taskExecutorResourceSpec);

		String metricQueryServiceAddress = metricRegistry.getMetricQueryServiceGatewayRpcAddress();

		// todo 创建task Executor
		return new TaskExecutor(
			rpcService,
			taskManagerConfiguration,
			highAvailabilityServices,
			taskManagerServices,
			heartbeatServices,
			taskManagerMetricGroup.f0,
			metricQueryServiceAddress,
			blobCacheService,
			fatalErrorHandler,
			new TaskExecutorPartitionTrackerImpl(taskManagerServices.getShuffleEnvironment()),
			createBackPressureSampleService(configuration, rpcService.getScheduledExecutor()));
	}

	static BackPressureSampleService createBackPressureSampleService(
			Configuration configuration,
			ScheduledExecutor scheduledExecutor) {
		return new BackPressureSampleService(
			configuration.getInteger(WebOptions.BACKPRESSURE_NUM_SAMPLES),
			Time.milliseconds(configuration.getInteger(WebOptions.BACKPRESSURE_DELAY)),
			scheduledExecutor);
	}

	/**
	 * Create a RPC service for the task manager.
	 *
	 * @param configuration The configuration for the TaskManager.
	 * @param haServices to use for the task manager hostname retrieval
	 */
	public static RpcService createRpcService(
			final Configuration configuration,
			final HighAvailabilityServices haServices) throws Exception {

		checkNotNull(configuration);
		checkNotNull(haServices);

		final String taskManagerAddress = determineTaskManagerBindAddress(configuration, haServices);
		final String portRangeDefinition = configuration.getString(TaskManagerOptions.RPC_PORT);

		return AkkaRpcServiceUtils.createRpcService(taskManagerAddress, portRangeDefinition, configuration);
	}

	private static String determineTaskManagerBindAddress(
			final Configuration configuration,
			final HighAvailabilityServices haServices) throws Exception {

		final String configuredTaskManagerHostname = configuration.getString(TaskManagerOptions.HOST);

		if (configuredTaskManagerHostname != null) {
			LOG.info("Using configured hostname/address for TaskManager: {}.", configuredTaskManagerHostname);
			return configuredTaskManagerHostname;
		} else {
			return determineTaskManagerBindAddressByConnectingToResourceManager(configuration, haServices);
		}
	}

	private static String determineTaskManagerBindAddressByConnectingToResourceManager(
			final Configuration configuration,
			final HighAvailabilityServices haServices) throws LeaderRetrievalException {

		final Duration lookupTimeout = AkkaUtils.getLookupTimeout(configuration);

		final InetAddress taskManagerAddress = LeaderRetrievalUtils.findConnectingAddress(
			haServices.getResourceManagerLeaderRetriever(),
			lookupTimeout);

		LOG.info("TaskManager will use hostname/address '{}' ({}) for communication.",
			taskManagerAddress.getHostName(), taskManagerAddress.getHostAddress());

		HostBindPolicy bindPolicy = HostBindPolicy.fromString(configuration.getString(TaskManagerOptions.HOST_BIND_POLICY));
		return bindPolicy == HostBindPolicy.IP ? taskManagerAddress.getHostAddress() : taskManagerAddress.getHostName();
	}
}
