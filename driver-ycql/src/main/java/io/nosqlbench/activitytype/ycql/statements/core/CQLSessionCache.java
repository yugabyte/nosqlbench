package io.nosqlbench.activitytype.ycql.statements.core;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import io.nosqlbench.activitytype.ycql.core.CQLOptions;
import io.nosqlbench.activitytype.ycql.core.ProxyTranslator;
import io.nosqlbench.engine.api.activityapi.core.Shutdownable;
import io.nosqlbench.engine.api.activityimpl.ActivityDef;
import io.nosqlbench.engine.api.metrics.ActivityMetrics;
import io.nosqlbench.engine.api.scripting.ExprEvaluator;
import io.nosqlbench.engine.api.scripting.GraalJsEvaluator;
import io.nosqlbench.engine.api.util.SSLKsFactory;
import io.nosqlbench.nb.api.errors.BasicError;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class CQLSessionCache implements Shutdownable {

    private final static Logger logger = LogManager.getLogger(CQLSessionCache.class);
    private final static String DEFAULT_SESSION_ID = "default";
    private static final CQLSessionCache instance = new CQLSessionCache();
    private final Map<String, Session> sessionCache = new HashMap<>();

    private CQLSessionCache() {
    }

    public static CQLSessionCache get() {
        return instance;
    }

    public void stopSession(ActivityDef activityDef) {
        String key = activityDef.getParams().getOptionalString("clusterid").orElse(DEFAULT_SESSION_ID);
        Session session = sessionCache.get(key);
        session.getCluster().close();
        session.close();
    }

    public Session getSession(ActivityDef activityDef) {
        String key = activityDef.getParams().getOptionalString("clusterid").orElse(DEFAULT_SESSION_ID);
        return sessionCache.computeIfAbsent(key, (cid) -> createSession(activityDef, key));
    }

    // cbopts=\".withLoadBalancingPolicy(LatencyAwarePolicy.builder(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(\"dc1-us-east\", 0, false))).build()).withRetryPolicy(new LoggingRetryPolicy(DefaultRetryPolicy.INSTANCE))\"

    private Session createSession(ActivityDef activityDef, String sessid) {

        String host = activityDef.getParams().getOptionalString("host").orElse("localhost");
        int port = activityDef.getParams().getOptionalInteger("port").orElse(9042);

        String driverType = activityDef.getParams().getOptionalString("cqldriver").orElse("oss");

        if (!driverType.equals("oss")) {
            throw new BasicError("This driver (cqlv3) does not use the cqldriver option. It only initializes sessions through the OSS API path via the Cluster.builder() chain. Thus, the setting of '" + driverType + "' is not possible.");
        }

        Cluster.Builder builder = Cluster.builder();
        logger.info("Using driver type '" + driverType.toUpperCase() + "'");

        Optional<String> scb = activityDef.getParams()
            .getOptionalString("secureconnectbundle");
        scb.map(File::new)
            .ifPresent(builder::withCloudSecureConnectBundle);

        activityDef.getParams()
            .getOptionalString("insights")
            .map(Boolean::parseBoolean)
            .ifPresent(b -> {
                throw new BasicError("This driver (cqlv3) does not support the insights reporting feature.");
            });

        String[] contactPoints = activityDef.getParams().getOptionalString("host")
            .map(h -> h.split(",")).orElse(null);

        if (contactPoints == null) {
            contactPoints = activityDef.getParams().getOptionalString("hosts")
                .map(h -> h.split(",")).orElse(null);
        }
        if (contactPoints == null && scb.isEmpty()) {
            contactPoints = new String[]{"localhost"};
        }

        if (contactPoints != null) {
            builder.addContactPoints(contactPoints);
        }

        activityDef.getParams().getOptionalInteger("port").ifPresent(builder::withPort);

        builder.withCompression(ProtocolOptions.Compression.NONE);

        Optional<String> usernameOpt = activityDef.getParams().getOptionalString("username");
        Optional<String> passwordOpt = activityDef.getParams().getOptionalString("password");
        Optional<String> passfileOpt = activityDef.getParams().getOptionalString("passfile");

        if (usernameOpt.isPresent()) {
            String username = usernameOpt.get();
            String password;
            if (passwordOpt.isPresent()) {
                password = passwordOpt.get();
            } else if (passfileOpt.isPresent()) {
                Path path = Paths.get(passfileOpt.get());
                try {
                    password = Files.readAllLines(path).get(0);
                } catch (IOException e) {
                    String error = "Error while reading password from file:" + passfileOpt;
                    logger.error(error, e);
                    throw new RuntimeException(e);
                }
            } else {
                String error = "username is present, but neither password nor passfile are defined.";
                logger.error(error);
                throw new RuntimeException(error);
            }
            builder.withCredentials(username, password);
        }

        Optional<String> clusteropts = activityDef.getParams().getOptionalString("cbopts");
        if (clusteropts.isPresent()) {
            try {
                logger.info("applying cbopts:" + clusteropts.get());
                ExprEvaluator<Cluster.Builder> clusterEval = new GraalJsEvaluator<>(Cluster.Builder.class);
                clusterEval.put("builder", builder);
                String importEnv =
                    "load(\"nashorn:mozilla_compat.js\");\n" +
                        " importPackage(com.google.common.collect.Lists);\n" +
                        " importPackage(com.google.common.collect.Maps);\n" +
                        " importPackage(com.datastax.driver);\n" +
                        " importPackage(com.datastax.driver.core);\n" +
                        " importPackage(com.datastax.driver.core.policies);\n" +
                        "builder" + clusteropts.get() + "\n";
                clusterEval.script(importEnv);
                builder = clusterEval.eval();
                logger.info("successfully applied:" + clusteropts.get());
            } catch (Exception e) {
                throw new RuntimeException("Unable to evaluate: " + clusteropts.get() + " in script context:", e);
            }
        }

        if (activityDef.getParams().getOptionalString("whitelist").isPresent() &&
            activityDef.getParams().getOptionalString("lbp", "loadbalancingpolicy").isPresent()) {
            throw new BasicError("You specified both whitelist=.. and lbp=..., if you need whitelist and other policies together," +
                " be sure to use the lbp option only with a whitelist policy included.");
        }

        Optional<String> specSpec = activityDef.getParams()
            .getOptionalString("speculative");

        if (specSpec.isPresent()) {
            specSpec
                .map(speculative -> {
                    logger.info("speculative=>" + speculative);
                    return speculative;
                })
                .map(CQLOptions::speculativeFor)
                .ifPresent(builder::withSpeculativeExecutionPolicy);
        }

        activityDef.getParams().getOptionalString("protocol_version")
            .map(String::toUpperCase)
            .map(ProtocolVersion::valueOf)
            .map(pv -> {
                logger.info("protocol_version=>" + pv);
                return pv;
            })
            .ifPresent(builder::withProtocolVersion);

        activityDef.getParams().getOptionalString("socketoptions")
            .map(sockopts -> {
                logger.info("socketoptions=>" + sockopts);
                return sockopts;
            })
            .map(CQLOptions::socketOptionsFor)
            .ifPresent(builder::withSocketOptions);

        activityDef.getParams().getOptionalString("reconnectpolicy")
            .map(reconnectpolicy -> {
                logger.info("reconnectpolicy=>" + reconnectpolicy);
                return reconnectpolicy;
            })
            .map(CQLOptions::reconnectPolicyFor)
            .ifPresent(builder::withReconnectionPolicy);


        activityDef.getParams().getOptionalString("pooling")
            .map(pooling -> {
                logger.info("pooling=>" + pooling);
                return pooling;
            })
            .map(CQLOptions::poolingOptionsFor)
            .ifPresent(builder::withPoolingOptions);

        activityDef.getParams().getOptionalString("whitelist")
            .map(whitelist -> {
                logger.info("whitelist=>" + whitelist);
                return whitelist;
            })
            .map(p -> CQLOptions.whitelistFor(p, null))
            .ifPresent(builder::withLoadBalancingPolicy);

        activityDef.getParams().getOptionalString("lbp")
            .map(lbp -> {
                logger.info("lbp=>" + lbp);
                return lbp;
            })
            .map(p -> CQLOptions.lbpolicyFor(p, null))
            .ifPresent(builder::withLoadBalancingPolicy);

        activityDef.getParams().getOptionalString("tickduration")
            .map(tickduration -> {
                logger.info("tickduration=>" + tickduration);
                return tickduration;
            })
            .map(CQLOptions::withTickDuration)
            .ifPresent(builder::withNettyOptions);

        activityDef.getParams().getOptionalString("compression")
            .map(compression -> {
                logger.info("compression=>" + compression);
                return compression;
            })
            .map(CQLOptions::withCompression)
            .ifPresent(builder::withCompression);


        SSLContext context = SSLKsFactory.get().getContext(activityDef);
        if (context != null) {
            builder.withSSL(RemoteEndpointAwareJdkSSLOptions.builder().withSSLContext(context).build());
        }

        RetryPolicy retryPolicy = activityDef.getParams()
            .getOptionalString("retrypolicy")
            .map(CQLOptions::retryPolicyFor).orElse(DefaultRetryPolicy.INSTANCE);

        if (retryPolicy instanceof LoggingRetryPolicy) {
            logger.info("using LoggingRetryPolicy");
        }

        builder.withRetryPolicy(retryPolicy);

        if (!activityDef.getParams().getOptionalBoolean("jmxreporting").orElse(false)) {
            builder.withoutJMXReporting();
        }

        // Proxy Translator and Whitelist for use with DS Cloud on-demand single-endpoint setup
        if (activityDef.getParams().getOptionalBoolean("single-endpoint").orElse(false)) {
            InetSocketAddress inetHost = new InetSocketAddress(host, port);
            final List<InetSocketAddress> whiteList = new ArrayList<>();
            whiteList.add(inetHost);

            LoadBalancingPolicy whitelistPolicy = new WhiteListPolicy(new RoundRobinPolicy(), whiteList);
            builder.withAddressTranslator(new ProxyTranslator(inetHost)).withLoadBalancingPolicy(whitelistPolicy);
        }

        activityDef.getParams().getOptionalString("haproxy_source_ip").map(
            ip -> {
                return new NettyOptions() {
                    @Override
                    public void afterChannelInitialized(SocketChannel channel) throws Exception {
                        try {
                            InetAddress sourceIp = InetAddress.getByName(ip);
                            InetAddress destIp = activityDef.getParams().getOptionalString("haproxy_dest_ip").map(destip -> {
                                    try {
                                        return InetAddress.getByName(destip);
                                    } catch (UnknownHostException e) {
                                        logger.warn("Invalid haproxy_dest_ip {}", destip);
                                        return sourceIp;
                                    }
                                }
                            ).orElse(sourceIp);

                            channel.pipeline().addFirst("proxyProtocol", new ProxyProtocolHander(
                                new HAProxyMessage(
                                    HAProxyProtocolVersion.V1,
                                    HAProxyCommand.PROXY,
                                    sourceIp instanceof Inet6Address ? HAProxyProxiedProtocol.TCP6 : HAProxyProxiedProtocol.TCP4,
                                    sourceIp.getHostAddress(),
                                    destIp.getHostAddress(),
                                    8000,
                                    8000)));
                        } catch (UnknownHostException e) {
                            logger.warn("Invalid haproxy_source_ip {}", ip);
                        }
                    }
                };
            }
        ).ifPresent(builder::withNettyOptions);

        Cluster cl = builder.build();

        // Apply default idempotence, if set
        activityDef.getParams().getOptionalBoolean("defaultidempotence").map(
            b -> cl.getConfiguration().getQueryOptions().setDefaultIdempotence(b)
        );

        Session session = cl.newSession();

        // This also forces init of metadata

        logger.info("cluster-metadata-allhosts:\n" + session.getCluster().getMetadata().getAllHosts());

        if (activityDef.getParams().getOptionalBoolean("drivermetrics").orElse(false)) {
            String driverPrefix = "driver." + sessid;
            driverPrefix = activityDef.getParams().getOptionalString("driverprefix").orElse(driverPrefix) + ".";
            ActivityMetrics.mountSubRegistry(driverPrefix, cl.getMetrics().getRegistry());
        }

        return session;
    }

    @Override
    public void shutdown() {
        for (Session session : sessionCache.values()) {
            Cluster cluster = session.getCluster();
            session.close();
            cluster.close();
        }
    }
}
