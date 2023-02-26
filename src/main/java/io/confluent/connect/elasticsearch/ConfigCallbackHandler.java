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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;

import org.apache.http.Header;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.KerberosCredentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.Lookup;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.message.BasicHeader;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.protocol.HttpContext;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.connect.errors.ConnectException;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.AwsSignerExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

import static org.apache.http.protocol.HttpCoreContext.HTTP_TARGET_HOST;

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
    if (config.isAwsSigned()) {
      log.debug("AWS Signing detected!");
      Aws4Signer signer = Aws4Signer.create();
      String serviceName = "es";
      String region = config.getString(ElasticsearchSinkConnectorConfig.AWS_REGION_CONFIG);
      HttpRequestInterceptor interceptor = new AwsRequestSigningApacheInterceptor(
              serviceName,
              signer,
              config.getCredentialsProvider(),
              region);
      builder.addInterceptorLast(interceptor);
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


  /**
   * An {@link HttpRequestInterceptor} that signs requests using any AWS {@link Signer}
   * and {@link AwsCredentialsProvider}.
   */
  class AwsRequestSigningApacheInterceptor implements HttpRequestInterceptor {
    /**
     * The service that we're connecting to.
     */
    private final String service;

    /**
     * The particular signer implementation.
     */
    private final Signer signer;

    /**
     * The source of AWS credentials for signing.
     */
    private final AwsCredentialsProvider awsCredentialsProvider;

    /**
     * The region signing region.
     */
    private final Region region;

    /**
     * @param service                service that we're connecting to
     * @param signer                 particular signer implementation
     * @param awsCredentialsProvider source of AWS credentials for signing
     * @param region                 signing region
     */
    public AwsRequestSigningApacheInterceptor(final String service,
                                              final Signer signer,
                                              final AwsCredentialsProvider awsCredentialsProvider,
                                              final Region region) {
      this.service = service;
      this.signer = signer;
      this.awsCredentialsProvider = awsCredentialsProvider;
      this.region = Objects.requireNonNull(region);
    }

    /**
     * @param service                service that we're connecting to
     * @param signer                 particular signer implementation
     * @param awsCredentialsProvider source of AWS credentials for signing
     * @param region                 signing region
     */
    public AwsRequestSigningApacheInterceptor(final String service,
                                              final Signer signer,
                                              final AwsCredentialsProvider awsCredentialsProvider,
                                              final String region) {
      this(service, signer, awsCredentialsProvider, Region.of(region));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void process(final HttpRequest request, final HttpContext context)
            throws HttpException, IOException {
      URIBuilder uriBuilder;
      try {
        uriBuilder = new URIBuilder(request.getRequestLine().getUri());
      } catch (URISyntaxException e) {
        throw new IOException("Invalid URI", e);
      }

      // Copy Apache HttpRequest to AWS Request
      SdkHttpFullRequest.Builder requestBuilder = SdkHttpFullRequest.builder()
              .method(SdkHttpMethod.fromValue(request.getRequestLine().getMethod()))
              .uri(buildUri(context, uriBuilder));

      if (request instanceof HttpEntityEnclosingRequest) {
        HttpEntityEnclosingRequest httpEntityEnclosingRequest =
                (HttpEntityEnclosingRequest) request;
        if (httpEntityEnclosingRequest.getEntity() != null) {
          InputStream content = httpEntityEnclosingRequest.getEntity().getContent();
          requestBuilder.contentStreamProvider(() -> content);
        }
      }
      requestBuilder.rawQueryParameters(nvpToMapParams(uriBuilder.getQueryParams()));
      requestBuilder.headers(headerArrayToMap(request.getAllHeaders()));

      ExecutionAttributes attributes = new ExecutionAttributes();
      attributes.putAttribute(AwsSignerExecutionAttribute.AWS_CREDENTIALS,
              awsCredentialsProvider.resolveCredentials());
      attributes.putAttribute(AwsSignerExecutionAttribute.SERVICE_SIGNING_NAME, service);
      attributes.putAttribute(AwsSignerExecutionAttribute.SIGNING_REGION, region);

      // Sign it
      SdkHttpFullRequest signedRequest = signer.sign(requestBuilder.build(), attributes);

      // Now copy everything back
      request.setHeaders(mapToHeaderArray(signedRequest.headers()));
      if (request instanceof HttpEntityEnclosingRequest) {
        HttpEntityEnclosingRequest httpEntityEnclosingRequest =
                (HttpEntityEnclosingRequest) request;
        if (httpEntityEnclosingRequest.getEntity() != null) {
          BasicHttpEntity basicHttpEntity = new BasicHttpEntity();
          basicHttpEntity.setContent(signedRequest.contentStreamProvider()
                  .orElseThrow(() -> new IllegalStateException("There must be content"))
                  .newStream());
          httpEntityEnclosingRequest.setEntity(basicHttpEntity);
        }
      }
    }

    private URI buildUri(final HttpContext context, URIBuilder uriBuilder) throws IOException {
      try {
        HttpHost host = (HttpHost) context.getAttribute(HTTP_TARGET_HOST);

        if (host != null) {
          uriBuilder.setHost(host.getHostName());
          uriBuilder.setScheme(host.getSchemeName());
          uriBuilder.setPort(host.getPort());
        }

        return uriBuilder.build();
      } catch (URISyntaxException e) {
        throw new IOException("Invalid URI", e);
      }
    }

    /**
     * @param params list of HTTP query params as NameValuePairs
     * @return a multimap of HTTP query params
     */
    private Map<String, List<String>> nvpToMapParams(final List<NameValuePair> params) {
      Map<String, List<String>> parameterMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      for (NameValuePair nvp : params) {
        List<String> argsList =
                parameterMap.computeIfAbsent(nvp.getName(), k -> new ArrayList<>());
        argsList.add(nvp.getValue());
      }
      return parameterMap;
    }

    /**
     * @param headers modelled Header objects
     * @return a Map of header entries
     */
    private Map<String, List<String>> headerArrayToMap(final Header[] headers) {
      Map<String, List<String>> headersMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      for (Header header : headers) {
        if (!skipHeader(header)) {
          headersMap.put(header.getName(), headersMap
                  .getOrDefault(header.getName(),
                          new LinkedList<>(Collections.singletonList(header.getValue()))));
        }
      }
      return headersMap;
    }

    /**
     * @param header header line to check
     * @return true if the given header should be excluded when signing
     */
    private boolean skipHeader(final Header header) {
      return ("content-length".equalsIgnoreCase(header.getName())
              && "0".equals(header.getValue())) // Strip Content-Length: 0
              || "host".equalsIgnoreCase(header.getName()); // Host comes from endpoint
    }

    /**
     * @param mapHeaders Map of header entries
     * @return modelled Header objects
     */
    private Header[] mapToHeaderArray(final Map<String, List<String>> mapHeaders) {
      Header[] headers = new Header[mapHeaders.size()];
      int i = 0;
      for (Map.Entry<String, List<String>> headerEntry : mapHeaders.entrySet()) {
        for (String value : headerEntry.getValue()) {
          headers[i++] = new BasicHeader(headerEntry.getKey(), value);
        }
      }
      return headers;
    }
  }
}
