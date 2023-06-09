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

package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.concurrent.akka.ActorSystemScheduledExecutorAdapter;
import org.apache.flink.runtime.concurrent.akka.AkkaFutureUtils;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.akka.exceptions.AkkaRpcRuntimeException;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.runtime.rpc.messages.HandshakeSuccessMessage;
import org.apache.flink.runtime.rpc.messages.RemoteHandshakeMessage;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.function.FunctionUtils;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.DeadLetter;
import akka.actor.Props;
import akka.pattern.Patterns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import scala.Option;
import scala.reflect.ClassTag$;

import static org.apache.flink.runtime.concurrent.akka.ClassLoadingUtils.guardCompletionWithContextClassLoader;
import static org.apache.flink.runtime.concurrent.akka.ClassLoadingUtils.runWithContextClassLoader;
import static org.apache.flink.runtime.concurrent.akka.ClassLoadingUtils.withContextClassLoader;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Akka based {@link RpcService} implementation. The RPC service starts an Akka actor to receive RPC
 * invocations from a {@link RpcGateway}.
 */
@ThreadSafe
public class AkkaRpcService implements RpcService {

    private static final Logger LOG = LoggerFactory.getLogger(AkkaRpcService.class);

    static final int VERSION = 2;

    private final Object lock = new Object();

    private final ActorSystem actorSystem;
    private final AkkaRpcServiceConfiguration configuration;

    private final ClassLoader flinkClassLoader;

    @GuardedBy("lock")
    private final Map<ActorRef, RpcEndpoint> actors = new HashMap<>(4);

    private final String address;
    private final int port;

    private final boolean captureAskCallstacks;

    private final ScheduledExecutor internalScheduledExecutor;

    private final CompletableFuture<Void> terminationFuture;

    private final Supervisor supervisor;

    private volatile boolean stopped;

    @VisibleForTesting
    public AkkaRpcService(
            final ActorSystem actorSystem, final AkkaRpcServiceConfiguration configuration) {
        // 创建 RpcService (AkkaRpcService)
        this(actorSystem, configuration, AkkaRpcService.class.getClassLoader());
    }

    AkkaRpcService(
            final ActorSystem actorSystem,
            final AkkaRpcServiceConfiguration configuration,
            final ClassLoader flinkClassLoader) {
        // akka 里面的 ActorSystem
        this.actorSystem = checkNotNull(actorSystem, "actor system");
        // akka RpcService 配置
        // 比如 akka.ask.timeout 超时时间 默认 10s
        // 比如 akka.framesize 一条消息限制大小 默认 10MB
        this.configuration = checkNotNull(configuration, "akka rpc service configuration");
        this.flinkClassLoader = checkNotNull(flinkClassLoader, "flinkClassLoader");

        // ActorSystem 地址
        // 比如 akka.tcp://flink@10.140.9.85:51269
        Address actorSystemAddress = AkkaUtils.getAddress(actorSystem);
        if (actorSystemAddress.host().isDefined()) {
            address = actorSystemAddress.host().get();
        } else {
            address = "";
        }
        if (actorSystemAddress.port().isDefined()) {
            port = (Integer) actorSystemAddress.port().get();
        } else {
            port = -1;
        }

        captureAskCallstacks = configuration.captureAskCallStack();

        // Akka always sets the threads context class loader to the class loader with which it was
        // loaded (i.e., the plugin class loader)
        // we must ensure that the context class loader is set to the Flink class loader when we
        // call into Flink
        // otherwise we could leak the plugin class loader or poison the context class loader of
        // external threads (because they inherit the current threads context class loader)
        internalScheduledExecutor =
                new ActorSystemScheduledExecutorAdapter(actorSystem, flinkClassLoader);
        terminationFuture = new CompletableFuture<>();
        stopped = false;

        // 创建 Supervisor 主管 Actor (后续创建 Actor 由 Supervisor 管生要管养 管杀要管埋 )
        // 比如 Actor 路径
        // 1 akka.tcp://${actor_system}@${actor_system_host}:${actor_system_port}/user/${actor_name}
        // 2 akka://flink/user/rpc
        supervisor = startSupervisorActor();
        startDeadLettersActor();
    }

    private void startDeadLettersActor() {
        final ActorRef deadLettersActor =
                actorSystem.actorOf(DeadLettersActor.getProps(), "deadLettersActor");
        actorSystem.eventStream().subscribe(deadLettersActor, DeadLetter.class);
    }

    private Supervisor startSupervisorActor() {
        final ExecutorService terminationFutureExecutor =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory(
                                "AkkaRpcService-Supervisor-Termination-Future-Executor"));
        // 从 ActorSystem 创建 Supervisor 主管 Actor
        final ActorRef actorRef =
                SupervisorActor.startSupervisorActor(
                        actorSystem,
                        withContextClassLoader(terminationFutureExecutor, flinkClassLoader));

        // Supervisor 封装 Actor
        return Supervisor.create(actorRef, terminationFutureExecutor);
    }

    public ActorSystem getActorSystem() {
        return actorSystem;
    }

    protected int getVersion() {
        return VERSION;
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    public int getPort() {
        return port;
    }

    // this method does not mutate state and is thus thread-safe
    @Override
    public <C extends RpcGateway> CompletableFuture<C> connect(
            // 远程 akka RPC 地址
            // 比如 akka.tcp://flink@10.140.9.85:60902/user/rpc/cc9d2c7e-2f1f-457c-9ea3-11a6b67a1591
            final String address,
            // 远程 akka rpc gateway (也即协议接口)
            final Class<C> clazz) {

        // 连接远程 RPC (Actor)
        return connectInternal(
                address,
                clazz,
                (ActorRef actorRef) -> {
                    Tuple2<String, String> addressHostname = extractAddressHostname(actorRef);

                    return new AkkaInvocationHandler(
                            addressHostname.f0,
                            addressHostname.f1,
                            actorRef,
                            configuration.getTimeout(),
                            configuration.getMaximumFramesize(),
                            configuration.isForceRpcInvocationSerialization(),
                            null,
                            captureAskCallstacks,
                            flinkClassLoader);
                });
    }

    // this method does not mutate state and is thus thread-safe
    @Override
    public <F extends Serializable, C extends FencedRpcGateway<F>> CompletableFuture<C> connect(
            String address, F fencingToken, Class<C> clazz) {
        return connectInternal(
                address,
                clazz,
                (ActorRef actorRef) -> {
                    Tuple2<String, String> addressHostname = extractAddressHostname(actorRef);

                    return new FencedAkkaInvocationHandler<>(
                            addressHostname.f0,
                            addressHostname.f1,
                            actorRef,
                            configuration.getTimeout(),
                            configuration.getMaximumFramesize(),
                            configuration.isForceRpcInvocationSerialization(),
                            null,
                            () -> fencingToken,
                            captureAskCallstacks,
                            flinkClassLoader);
                });
    }

    @Override
    public <C extends RpcEndpoint & RpcGateway> RpcServer startServer(C rpcEndpoint) {
        checkNotNull(rpcEndpoint, "rpc endpoint");

        // 1 向 Supervisor 发送一个 ask 请求创建一个 Actor(拿到 ActorRef)
        // 2 缓存 ActorRef 对应的 RpcEndpoint 到 actions
        final SupervisorActor.ActorRegistration actorRegistration =
                registerAkkaRpcActor(rpcEndpoint);
        final ActorRef actorRef = actorRegistration.getActorRef();
        final CompletableFuture<Void> actorTerminationFuture =
                actorRegistration.getTerminationFuture();
        // akka[.tcp]://flink[@ip:port]/user/rpc/390dba5f-f1fe-475f-9cf6-b37eb5761266
        LOG.info(
                "Starting RPC endpoint for {} at {} .",
                rpcEndpoint.getClass().getName(),
                actorRef.path());
        // akka.tcp://flink@10.140.9.85:62856/user/rpc/390dba5f-f1fe-475f-9cf6-b37eb5761266
        final String akkaAddress = AkkaUtils.getAkkaURL(actorSystem, actorRef);
        final String hostname;
        Option<String> host = actorRef.path().address().host();
        if (host.isEmpty()) {
            hostname = "localhost";
        } else {
            hostname = host.get();
        }

        // 获取 RpcEndpoint 实现哪些接口类
        // 比如 0 -> GetNowGateway , 1 -> RpcGateway
        Set<Class<?>> implementedRpcGateways =
                new HashSet<>(RpcUtils.extractImplementedRpcGateways(rpcEndpoint.getClass()));
        // 添加其他接口
        implementedRpcGateways.add(RpcServer.class);
        implementedRpcGateways.add(AkkaBasedEndpoint.class);

        final InvocationHandler akkaInvocationHandler;

        // 判断 RpcEndpoint 的类型
        if (rpcEndpoint instanceof FencedRpcEndpoint) {
            // a FencedRpcEndpoint needs a FencedAkkaInvocationHandler
            // 创建 FencedAkkaInvocationHandler
            akkaInvocationHandler =
                    new FencedAkkaInvocationHandler<>(
                            akkaAddress,
                            hostname,
                            actorRef,
                            configuration.getTimeout(),
                            configuration.getMaximumFramesize(),
                            configuration.isForceRpcInvocationSerialization(),
                            actorTerminationFuture,
                            ((FencedRpcEndpoint<?>) rpcEndpoint)::getFencingToken,
                            captureAskCallstacks,
                            flinkClassLoader);
        } else {
            // 创建 AkkaInvocationHandler
            akkaInvocationHandler =
                    new AkkaInvocationHandler(
                            akkaAddress,
                            hostname,
                            actorRef,
                            configuration.getTimeout(),
                            configuration.getMaximumFramesize(),
                            configuration.isForceRpcInvocationSerialization(),
                            actorTerminationFuture,
                            captureAskCallstacks,
                            flinkClassLoader);
        }

        // Rather than using the System ClassLoader directly, we derive the ClassLoader
        // from this class . That works better in cases where Flink runs embedded and all Flink
        // code is loaded dynamically (for example from an OSGI bundle) through a custom ClassLoader
        ClassLoader classLoader = getClass().getClassLoader();

        @SuppressWarnings("unchecked")
        // 基于 JDK 动态代理获取 RpcServer
        /**
         * 1 classLoader 类加载器
         * 2 Actor 实现哪些 RpcGateway 接口
         * 3 akkaInvocationHandler 代理器 核心方法 invoke()
         */
                RpcServer server =
                (RpcServer)
                        Proxy.newProxyInstance(
                                classLoader,
                                implementedRpcGateways.toArray(
                                        new Class<?>[implementedRpcGateways.size()]),
                                akkaInvocationHandler);

        return server;
    }

    private <C extends RpcEndpoint & RpcGateway>
    SupervisorActor.ActorRegistration registerAkkaRpcActor(C rpcEndpoint) {
        final Class<? extends AbstractActor> akkaRpcActorType;

        // 根据 RpcEndpoint 类型创建对应的 AkkaRpcActor (继承 akka 的 AbstractActor)
        if (rpcEndpoint instanceof FencedRpcEndpoint) {
            akkaRpcActorType = FencedAkkaRpcActor.class;
        } else {
            akkaRpcActorType = AkkaRpcActor.class;
        }

        synchronized (lock) {
            checkState(!stopped, "RpcService is stopped");

            final SupervisorActor.StartAkkaRpcActorResponse startAkkaRpcActorResponse =
                    // 向 Supervisor 发送一个 ask 请求创建一个 Actor
                    SupervisorActor.startAkkaRpcActor(
                            supervisor.getActor(),
                            actorTerminationFuture ->
                                    Props.create(
                                            akkaRpcActorType,
                                            rpcEndpoint,
                                            actorTerminationFuture,
                                            getVersion(),
                                            configuration.getMaximumFramesize(),
                                            configuration.isForceRpcInvocationSerialization(),
                                            flinkClassLoader),
                            // 创建 Actor 名称
                            rpcEndpoint.getEndpointId());

            final SupervisorActor.ActorRegistration actorRegistration =
                    startAkkaRpcActorResponse.orElseThrow(
                            cause ->
                                    new AkkaRpcRuntimeException(
                                            String.format(
                                                    "Could not create the %s for %s.",
                                                    AkkaRpcActor.class.getSimpleName(),
                                                    rpcEndpoint.getEndpointId()),
                                            cause));

            // 缓存 ActorRef 对应的 RpcEndpoint
            actors.put(actorRegistration.getActorRef(), rpcEndpoint);

            return actorRegistration;
        }
    }

    @Override
    public <F extends Serializable> RpcServer fenceRpcServer(RpcServer rpcServer, F fencingToken) {
        if (rpcServer instanceof AkkaBasedEndpoint) {

            InvocationHandler fencedInvocationHandler =
                    new FencedAkkaInvocationHandler<>(
                            rpcServer.getAddress(),
                            rpcServer.getHostname(),
                            ((AkkaBasedEndpoint) rpcServer).getActorRef(),
                            configuration.getTimeout(),
                            configuration.getMaximumFramesize(),
                            configuration.isForceRpcInvocationSerialization(),
                            null,
                            () -> fencingToken,
                            captureAskCallstacks,
                            flinkClassLoader);

            // Rather than using the System ClassLoader directly, we derive the ClassLoader
            // from this class . That works better in cases where Flink runs embedded and all Flink
            // code is loaded dynamically (for example from an OSGI bundle) through a custom
            // ClassLoader
            ClassLoader classLoader = getClass().getClassLoader();

            return (RpcServer)
                    Proxy.newProxyInstance(
                            classLoader,
                            new Class<?>[]{RpcServer.class, AkkaBasedEndpoint.class},
                            fencedInvocationHandler);
        } else {
            throw new RuntimeException(
                    "The given RpcServer must implement the AkkaGateway in order to fence it.");
        }
    }

    @Override
    public void stopServer(RpcServer selfGateway) {
        if (selfGateway instanceof AkkaBasedEndpoint) {
            final AkkaBasedEndpoint akkaClient = (AkkaBasedEndpoint) selfGateway;
            final RpcEndpoint rpcEndpoint;

            synchronized (lock) {
                if (stopped) {
                    return;
                } else {
                    rpcEndpoint = actors.remove(akkaClient.getActorRef());
                }
            }

            if (rpcEndpoint != null) {
                terminateAkkaRpcActor(akkaClient.getActorRef(), rpcEndpoint);
            } else {
                LOG.debug(
                        "RPC endpoint {} already stopped or from different RPC service",
                        selfGateway.getAddress());
            }
        }
    }

    @Override
    public CompletableFuture<Void> stopService() {
        final CompletableFuture<Void> akkaRpcActorsTerminationFuture;

        synchronized (lock) {
            if (stopped) {
                return terminationFuture;
            }

            LOG.info("Stopping Akka RPC service.");

            stopped = true;

            akkaRpcActorsTerminationFuture = terminateAkkaRpcActors();
        }

        final CompletableFuture<Void> supervisorTerminationFuture =
                FutureUtils.composeAfterwards(
                        akkaRpcActorsTerminationFuture, supervisor::closeAsync);

        final CompletableFuture<Void> actorSystemTerminationFuture =
                FutureUtils.composeAfterwards(
                        supervisorTerminationFuture,
                        () -> AkkaFutureUtils.toJava(actorSystem.terminate()));

        actorSystemTerminationFuture.whenComplete(
                (Void ignored, Throwable throwable) -> {
                    runWithContextClassLoader(
                            () -> FutureUtils.doForward(ignored, throwable, terminationFuture),
                            flinkClassLoader);

                    LOG.info("Stopped Akka RPC service.");
                });

        return terminationFuture;
    }

    @GuardedBy("lock")
    @Nonnull
    private CompletableFuture<Void> terminateAkkaRpcActors() {
        final Collection<CompletableFuture<Void>> akkaRpcActorTerminationFutures =
                new ArrayList<>(actors.size());

        for (Map.Entry<ActorRef, RpcEndpoint> actorRefRpcEndpointEntry : actors.entrySet()) {
            akkaRpcActorTerminationFutures.add(
                    terminateAkkaRpcActor(
                            actorRefRpcEndpointEntry.getKey(),
                            actorRefRpcEndpointEntry.getValue()));
        }
        actors.clear();

        return FutureUtils.waitForAll(akkaRpcActorTerminationFutures);
    }

    private CompletableFuture<Void> terminateAkkaRpcActor(
            ActorRef akkaRpcActorRef, RpcEndpoint rpcEndpoint) {
        akkaRpcActorRef.tell(ControlMessages.TERMINATE, ActorRef.noSender());

        return rpcEndpoint.getTerminationFuture();
    }

    @Override
    public CompletableFuture<Void> getTerminationFuture() {
        return terminationFuture;
    }

    @Override
    public ScheduledExecutor getScheduledExecutor() {
        return internalScheduledExecutor;
    }

    @Override
    public ScheduledFuture<?> scheduleRunnable(Runnable runnable, long delay, TimeUnit unit) {
        checkNotNull(runnable, "runnable");
        checkNotNull(unit, "unit");
        checkArgument(delay >= 0L, "delay must be zero or larger");

        return internalScheduledExecutor.schedule(runnable, delay, unit);
    }

    @Override
    public void execute(Runnable runnable) {
        getScheduledExecutor().execute(runnable);
    }

    @Override
    public <T> CompletableFuture<T> execute(Callable<T> callable) {
        return CompletableFuture.supplyAsync(
                FunctionUtils.uncheckedSupplier(callable::call), getScheduledExecutor());
    }

    // ---------------------------------------------------------------------------------------
    // Private helper methods
    // ---------------------------------------------------------------------------------------

    private Tuple2<String, String> extractAddressHostname(ActorRef actorRef) {
        final String actorAddress = AkkaUtils.getAkkaURL(actorSystem, actorRef);
        final String hostname;
        Option<String> host = actorRef.path().address().host();
        if (host.isEmpty()) {
            hostname = "localhost";
        } else {
            hostname = host.get();
        }

        return Tuple2.of(actorAddress, hostname);
    }

    private <C extends RpcGateway> CompletableFuture<C> connectInternal(
            final String address,
            final Class<C> clazz,
            Function<ActorRef, InvocationHandler> invocationHandlerFactory) {
        checkState(!stopped, "RpcService is stopped");

        LOG.debug(
                "Try to connect to remote RPC endpoint with address {}. Returning a {} gateway.",
                address,
                clazz.getName());

        // 基于 ActorSystem 连接指定远程 Address 对应的 Actor 返回 ActorRef
        final CompletableFuture<ActorRef> actorRefFuture = resolveActorAddress(address);

        // 拿到远程 RPC ActorRef 发送握手消息 (最终调用远程 AkkaRpcActor.createReceive() )
        final CompletableFuture<HandshakeSuccessMessage> handshakeFuture =
                actorRefFuture.thenCompose(
                        (ActorRef actorRef) ->
                                AkkaFutureUtils.toJava(
                                        Patterns.ask(
                                                        actorRef,
                                                        new RemoteHandshakeMessage(
                                                                clazz, getVersion()),
                                                        configuration.getTimeout().toMillis())
                                                .<HandshakeSuccessMessage>mapTo(
                                                        ClassTag$.MODULE$
                                                                .<HandshakeSuccessMessage>apply(
                                                                        HandshakeSuccessMessage
                                                                                .class))));
        // 基于 JDK 动态代理创建远程 RPC Actor 的代理对象 ActorRef(通讯 gateway 为传入的接口)
        final CompletableFuture<C> gatewayFuture =
                actorRefFuture.thenCombineAsync(
                        handshakeFuture,
                        (ActorRef actorRef, HandshakeSuccessMessage ignored) -> {
                            InvocationHandler invocationHandler =
                                    invocationHandlerFactory.apply(actorRef);

                            // Rather than using the System ClassLoader directly, we derive the
                            // ClassLoader from this class.
                            // That works better in cases where Flink runs embedded and
                            // all Flink code is loaded dynamically
                            // (for example from an OSGI bundle) through a custom ClassLoader
                            ClassLoader classLoader = getClass().getClassLoader();

                            @SuppressWarnings("unchecked")
                            C proxy =
                                    (C)
                                            Proxy.newProxyInstance(
                                                    classLoader,
                                                    new Class<?>[]{clazz},
                                                    invocationHandler);

                            return proxy;
                        },
                        actorSystem.dispatcher());

        // 返回远程 RPC Actor 代理对象 (ActorRef)
        return guardCompletionWithContextClassLoader(gatewayFuture, flinkClassLoader);
    }

    private CompletableFuture<ActorRef> resolveActorAddress(String address) {
        final ActorSelection actorSel = actorSystem.actorSelection(address);
        return actorSel.resolveOne(configuration.getTimeout())
                .toCompletableFuture()
                .exceptionally(
                        error -> {
                            throw new CompletionException(
                                    new RpcConnectionException(
                                            String.format(
                                                    "Could not connect to rpc endpoint under address %s.",
                                                    address),
                                            error));
                        });
    }

    // ---------------------------------------------------------------------------------------
    // Private inner classes
    // ---------------------------------------------------------------------------------------

    private static final class Supervisor implements AutoCloseableAsync {

        private final ActorRef actor;

        private final ExecutorService terminationFutureExecutor;

        private Supervisor(ActorRef actor, ExecutorService terminationFutureExecutor) {
            this.actor = actor;
            this.terminationFutureExecutor = terminationFutureExecutor;
        }

        private static Supervisor create(
                ActorRef actorRef, ExecutorService terminationFutureExecutor) {
            return new Supervisor(actorRef, terminationFutureExecutor);
        }

        public ActorRef getActor() {
            return actor;
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return ExecutorUtils.nonBlockingShutdown(
                    30L, TimeUnit.SECONDS, terminationFutureExecutor);
        }
    }
}
