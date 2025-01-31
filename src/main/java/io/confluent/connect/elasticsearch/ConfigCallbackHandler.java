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

import com.sun.security.auth.module.Krb5LoginModule;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.KerberosCredentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Lookup;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.kafka.common.network.ConnectionMode;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.connect.errors.ConnectException;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigCallbackHandler implements HttpClientConfigCallback {

  private static final Logger log = LoggerFactory.getLogger(ConfigCallbackHandler.class);

  private static final Oid SPNEGO_OID = spnegoOid();

  private final ElasticsearchSinkConnectorConfig config;

  public ConfigCallbackHandler(ElasticsearchSinkConnectorConfig config) {
    this.config = config;
  }

  /**
   * Customizes the client according to the configurations and starts the connection reaping thread.
   *
   * @param builder the HttpAsyncClientBuilder
   * @return the builder
   */
  @Override
  public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder builder) {
    RequestConfig requestConfig = RequestConfig.custom()
            .setContentCompressionEnabled(config.compression())
            .setConnectTimeout(config.connectionTimeoutMs())
            .setConnectionRequestTimeout(config.readTimeoutMs())
            .setSocketTimeout(config.readTimeoutMs())
            .build();

    builder.setConnectionManager(createConnectionManager())
            .setDefaultRequestConfig(requestConfig);

    configureAuthentication(builder);

    if (config.isKerberosEnabled()) {
      configureKerberos(builder);
    }

    if (config.isSslEnabled()) {
      configureSslContext(builder);
    }

    if (config.isKerberosEnabled() && config.isSslEnabled()) {
      log.info("Using Kerberos and SSL connection to {}.", config.connectionUrls());
    } else if (config.isKerberosEnabled()) {
      log.info("Using Kerberos connection to {}.", config.connectionUrls());
    } else if (config.isSslEnabled()) {
      log.info("Using SSL connection to {}.", config.connectionUrls());
    } else {
      log.info("Using unsecured connection to {}.", config.connectionUrls());
    }

    return builder;
  }

  /**
   * Configures HTTP authentication and proxy authentication according to the client configuration.
   *
   * @param builder the HttpAsyncClientBuilder
   */
  private void configureAuthentication(HttpAsyncClientBuilder builder) {
    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    if (config.isAuthenticatedConnection()) {
      config.connectionUrls().forEach(url -> credentialsProvider.setCredentials(
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
  private PoolingNHttpClientConnectionManager createConnectionManager() {
    try {
      PoolingNHttpClientConnectionManager cm;
      IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
              .setConnectTimeout(config.connectionTimeoutMs())
              .setSoTimeout(config.readTimeoutMs())
              .build();
      ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);

      if (config.isSslEnabled()) {
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

      // Allowing up to two http connections per processing thread to a given host
      int maxPerRoute = Math.max(10, config.maxInFlightRequests() * 2);
      cm.setDefaultMaxPerRoute(maxPerRoute);
      // And for the global limit, with multiply the per-host limit
      // by the number of potential different ES hosts
      cm.setMaxTotal(maxPerRoute * config.connectionUrls().size());

      log.debug("Connection pool config: maxPerRoute: {}, maxTotal {}",
              cm.getDefaultMaxPerRoute(),
              cm.getMaxTotal());

      return cm;
    } catch (IOReactorException e) {
      throw new ConnectException("Unable to open ElasticsearchClient.", e);
    }
  }

  /**
   * Configures the client to use Kerberos authentication. Overrides any proxy or basic auth
   * credentials.
   *
   * @param builder the HttpAsyncClientBuilder to configure
   * @return the configured builder
   */
  private HttpAsyncClientBuilder configureKerberos(HttpAsyncClientBuilder builder) {
    GSSManager gssManager = GSSManager.getInstance();
    Lookup<AuthSchemeProvider> authSchemeRegistry =
        RegistryBuilder.<AuthSchemeProvider>create()
            .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory())
            .build();
    builder.setDefaultAuthSchemeRegistry(authSchemeRegistry);

    try {
      LoginContext loginContext = loginContext();
      GSSCredential credential = Subject.doAs(
          loginContext.getSubject(),
          (PrivilegedExceptionAction<GSSCredential>) () -> gssManager.createCredential(
              null,
              GSSCredential.DEFAULT_LIFETIME,
              SPNEGO_OID,
              GSSCredential.INITIATE_ONLY
          )
      );
      CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(
          new AuthScope(
              AuthScope.ANY_HOST,
              AuthScope.ANY_PORT,
              AuthScope.ANY_REALM,
              AuthSchemes.SPNEGO
          ),
          new KerberosCredentials(credential)
      );
      builder.setDefaultCredentialsProvider(credentialsProvider);
    } catch (PrivilegedActionException e) {
      throw new ConnectException(e);
    }

    return builder;
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
    SslFactory sslFactory = new SslFactory(ConnectionMode.CLIENT, null, false);
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

  /**
   * Logs in and returns a login context for the given kerberos user principle.
   *
   * @return the login context
   * @throws PrivilegedActionException if the login failed
   */
  private LoginContext loginContext() throws PrivilegedActionException {
    Configuration conf = new Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        return new AppConfigurationEntry[] {
            new AppConfigurationEntry(
                Krb5LoginModule.class.getName(),
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                kerberosConfigs()
            )
        };
      }
    };

    return AccessController.doPrivileged(
        (PrivilegedExceptionAction<LoginContext>) () -> {
          Subject subject = new Subject(
              false,
              Collections.singleton(new KerberosPrincipal(config.kerberosUserPrincipal())),
              new HashSet<>(),
              new HashSet<>()
          );
          LoginContext loginContext = new LoginContext(
              "ElasticsearchSinkConnector",
              subject,
              null,
              conf
          );
          loginContext.login();
          return loginContext;
        }
    );
  }

  /**
   * Creates the Kerberos configurations.
   *
   * @return map of kerberos configs
   */
  private Map<String, Object> kerberosConfigs() {
    Map<String, Object> configs = new HashMap<>();
    configs.put("useTicketCache", "true");
    configs.put("renewTGT", "true");
    configs.put("useKeyTab", "true");
    configs.put("keyTab", config.keytabPath());
    //Krb5 in GSS API needs to be refreshed so it does not throw the error
    //Specified version of key is not available
    configs.put("refreshKrb5Config", "true");
    configs.put("principal", config.kerberosUserPrincipal());
    configs.put("storeKey", "false");
    configs.put("doNotPrompt", "true");
    return configs;
  }

  private static Oid spnegoOid() {
    try {
      return new Oid("1.3.6.1.5.5.2");
    } catch (GSSException gsse) {
      throw new ConnectException(gsse);
    }
  }
}
