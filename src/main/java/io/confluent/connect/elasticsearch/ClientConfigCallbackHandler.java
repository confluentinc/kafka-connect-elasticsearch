/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.elasticsearch;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.connect.errors.ConnectException;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientConfigCallbackHandler implements HttpClientConfigCallback {

  private static final Logger log = LoggerFactory.getLogger(ClientConfigCallbackHandler.class);

  private final ElasticsearchSinkConnectorConfig config;
  private final NHttpClientConnectionManager connectionManager;

  public ClientConfigCallbackHandler(ElasticsearchSinkConnectorConfig config) {
    this.config = config;
    this.connectionManager = configureConnectionManager();
  }

  /**
   * Customizes the client according to the configurations and starts the connection reaping thread.
   *
   * @param builder the HttpAsyncClientBuilder
   * @return the builder
   */
  @Override
  public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder builder) {
    builder.setConnectionManager(connectionManager);
    builder.setMaxConnPerRoute(config.maxInFlightRequests());
    builder.setMaxConnTotal(config.maxInFlightRequests());
    configureAuthentication(builder);

    if (config.secured()) {
      log.info("Using secured connection to {}.", config.connectionUrls());
      configureSslContext(builder);
    } else {
      log.info("Using unsecured connection to {}.", config.connectionUrls());
    }

    return builder;
  }

  /**
   * Returns the configured connection manager.
   *
   * @return the connection manager for the client.
   */
  public NHttpClientConnectionManager connectionManager() {
    return connectionManager;
  }

  /**
   * Configures HTTP authentication and proxy authentication according to the client configuration.
   *
   * @param builder the HttpAsyncClientBuilder
   */
  private void configureAuthentication(HttpAsyncClientBuilder builder) {
    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    if (config.isAuthenticatedConnection()) {
      config.connectionUrls().forEach(
          url -> credentialsProvider.setCredentials(
              new AuthScope(HttpHost.create(url)),
              new UsernamePasswordCredentials(config.username(), config.password().value())
          )
      );
      builder.setDefaultCredentialsProvider(credentialsProvider);
    }

    if (config.isBasicProxyConfigured()) {
      HttpHost proxy = new HttpHost(config.proxyHost(), config.proxyPort());
      builder.setProxy(proxy);

      if (config.isProxyWithAuthenticationConfigured()) {
        credentialsProvider.setCredentials(
            new AuthScope(proxy),
            new UsernamePasswordCredentials(config.proxyUsername(), config.proxyPassword().value())
        );
      }

      builder.setDefaultCredentialsProvider(credentialsProvider);
    }
  }

  /**
   * Creates a connection manager for the client.
   *
   * @return the connection manager
   */
  private PoolingNHttpClientConnectionManager configureConnectionManager() {
    try {
      PoolingNHttpClientConnectionManager cm;
      ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor();

      if (config.secured()) {
        HostnameVerifier hostnameVerifier = config.shouldDisableHostnameVerification()
            ? new NoopHostnameVerifier()
            : SSLConnectionSocketFactory.getDefaultHostnameVerifier();
        Registry<SchemeIOSessionStrategy> reg = RegistryBuilder.<SchemeIOSessionStrategy>create()
            .register("http", NoopIOSessionStrategy.INSTANCE)
            .register("https", new SSLIOSessionStrategy(sslContext(), hostnameVerifier))
            .build();

        cm = new PoolingNHttpClientConnectionManager(ioReactor, reg);
      } else {
        cm = new PoolingNHttpClientConnectionManager(ioReactor);
      }

      cm.setDefaultMaxPerRoute(config.maxInFlightRequests());
      cm.setMaxTotal(config.maxInFlightRequests());

      return cm;
    } catch (IOReactorException e) {
      throw new ConnectException("Unable to open ElasticsearchClient.", e);
    }
  }

  /**
   * Configures the client to use SSL if configured.
   *
   * @param builder the HttpAsyncClientBuilder
   */
  private void configureSslContext(HttpAsyncClientBuilder builder) {
    HostnameVerifier hostnameVerifier = config.shouldDisableHostnameVerification()
        ? new NoopHostnameVerifier()
        : SSLConnectionSocketFactory.getDefaultHostnameVerifier();

    SSLContext sslContext = sslContext();
    builder.setSSLContext(sslContext);
    builder.setSSLHostnameVerifier(hostnameVerifier);
    builder.setSSLStrategy(new SSLIOSessionStrategy(sslContext, hostnameVerifier));
  }

  /**
   * Gets the SslContext for the client.
   */
  private SSLContext sslContext() {
    SslFactory sslFactory = new SslFactory(Mode.CLIENT, null, false);
    sslFactory.configure(config.sslConfigs());

    try {
      // try AK <= 2.2 first
      log.debug("Trying AK 2.2 SslFactory methods.");
      return (SSLContext) SslFactory.class.getDeclaredMethod("sslContext").invoke(sslFactory);
    } catch (Exception e) {
      // must be running AK 2.3+
      log.debug("Could not find AK 2.2 SslFactory methods. Trying AK 2.3+ methods for SslFactory.");

      Object sslEngine;
      try {
        // try AK <= 2.6 second
        sslEngine = SslFactory.class.getDeclaredMethod("sslEngineBuilder").invoke(sslFactory);
        log.debug("Using AK 2.2-2.5 SslFactory methods.");
      } catch (Exception ex) {
        // must be running AK 2.6+
        log.debug(
            "Could not find AK 2.3-2.5 SslFactory methods. Trying AK 2.6+ methods for SslFactory."
        );
        try {
          sslEngine = SslFactory.class.getDeclaredMethod("sslEngineFactory").invoke(sslFactory);
          log.debug("Using AK 2.6+ SslFactory methods.");
        } catch (Exception exc) {
          throw new ConnectException("Failed to find methods for SslFactory.", exc);
        }
      }

      try {
        return (SSLContext) sslEngine
            .getClass()
            .getDeclaredMethod("sslContext")
            .invoke(sslEngine);
      } catch (Exception ex) {
        throw new ConnectException("Could not create SSLContext.", ex);
      }
    }
  }
}
