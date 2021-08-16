/*
 * Copyright 2020 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connect.kafka;

import com.couchbase.client.core.env.*;
import com.couchbase.client.core.error.CollectionExistsException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.couchbase.connect.kafka.config.common.CommonConfig;
import com.couchbase.connect.kafka.util.ScopeAndCollection;
import org.apache.kafka.common.config.ConfigException;

import java.io.Closeable;
import java.nio.file.Paths;
import java.util.*;

import static com.couchbase.client.core.env.IoConfig.networkResolution;
import static com.couchbase.client.core.env.TimeoutConfig.connectTimeout;
import static com.couchbase.client.core.util.CbStrings.isNullOrEmpty;
import static com.couchbase.client.java.ClusterOptions.clusterOptions;
import static java.util.Collections.emptyMap;

public class KafkaCouchbaseClient implements Closeable {
    private final ClusterEnvironment env;
    private final Cluster cluster;
    private final Map<String, Bucket> buckets = new Hashtable<>();
    private final List<String> scopes = new ArrayList<>();
    private final List<String> collections = new ArrayList<>();


    public KafkaCouchbaseClient(CommonConfig config) {
        this(config, emptyMap());
    }

    public KafkaCouchbaseClient(CommonConfig config, Map<String, String> clusterEnvProps) {
        List<String> clusterAddress = config.seedNodes();
        String connectionString = String.join(",", clusterAddress);
        NetworkResolution networkResolution = NetworkResolution.valueOf(config.network());

        SecurityConfig.Builder securityConfig = SecurityConfig.builder()
                .enableTls(config.enableTls())
                .enableHostnameVerification(config.enableHostnameVerification());
        if (!isNullOrEmpty(config.trustStorePath())) {
            securityConfig.trustStore(Paths.get(config.trustStorePath()), config.trustStorePassword().value(), Optional.empty());
        }
        if (!isNullOrEmpty(config.trustCertificatePath())) {
            securityConfig.trustCertificate(Paths.get(config.trustCertificatePath()));
        }

        ClusterEnvironment.Builder envBuilder = ClusterEnvironment.builder()
                .securityConfig(securityConfig)
                .ioConfig(networkResolution(networkResolution))
                .timeoutConfig(connectTimeout(config.bootstrapTimeout()));

        applyCustomEnvironmentProperties(envBuilder, clusterEnvProps);

        env = envBuilder.build();

        Authenticator authenticator = isNullOrEmpty(config.clientCertificatePath())
                ? PasswordAuthenticator.create(config.username(), config.password().value())
                : CertificateAuthenticator.fromKeyStore(Paths.get(config.clientCertificatePath()), config.clientCertificatePassword().value(), Optional.empty());

        cluster = Cluster.connect(connectionString,
                clusterOptions(authenticator)
                        .environment(env));

        buckets.put("__DEFAULT__", cluster.bucket(config.bucket()));
    }

    private static void applyCustomEnvironmentProperties(ClusterEnvironment.Builder envBuilder, Map<String, String> envProps) {
        try {
            envBuilder.load(new AbstractMapPropertyLoader<CoreEnvironment.Builder>() {
                @Override
                protected Map<String, String> propertyMap() {
                    return envProps;
                }
            });
        } catch (Exception e) {
            throw new ConfigException("Failed to apply Couchbase environment properties; " + e.getMessage());
        }
    }

    public ClusterEnvironment env() {
        return env;
    }

    public Cluster cluster() {
        return cluster;
    }

    public Bucket bucket() {
        return buckets.get("__DEFAULT__");
    }

    public Collection collection(ScopeAndCollection scopeAndCollection) {
        return bucket()
                .scope(scopeAndCollection.getScope())
                .collection(scopeAndCollection.getCollection());
    }

    public Collection collection(String bucket, String scope, String collection) {
        Bucket b;

        if (bucket == null) {
            b = bucket();
        } else {
            if (!buckets.containsKey(bucket)) {
                buckets.put(bucket, cluster.bucket(bucket));
            }
            b = buckets.get(bucket);
        }

        if (!scopes.contains(scope)) {
            if (!b.collections().getAllScopes().contains(ScopeSpec.create(scope))) {
                b.collections().createScope(scope);
            }
            scopes.add(scope);
        }

        if (!collections.contains(collection)) {
            try {
                b.collections().createCollection(CollectionSpec.create(collection, scope));
            } catch (CollectionExistsException e) {
            }
            collections.add(collection);
        }

        return b.scope(scope).collection(collection);
    }

    @Override
    public void close() {
        cluster.disconnect();
        env.shutdown();
    }
}
