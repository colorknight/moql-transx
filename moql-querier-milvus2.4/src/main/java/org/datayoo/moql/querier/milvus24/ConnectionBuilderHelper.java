package org.datayoo.moql.querier.milvus24;

import io.milvus.v2.client.ConnectConfig;
import org.apache.commons.lang3.Validate;

import java.util.Properties;

/**
 * @author tangtadin
 * @version 1.0
 * @description: TODO
 * @date 2023/2/23 12:40
 */
public abstract class ConnectionBuilderHelper {

  public static final String PROP_URI = "milvus.connection.uri";
  public static final String PROP_IDLE_TIMEOUT = "milvus.connection.idleTimeout";

  public static final String PROP_CONNECTION_TIMEOUT = "milvus.connection.connectionTimeout";

  public static final String PROP_KEEPALIVE_TIMEOUT = "milvus.connection.keepaliveTimeout";

  public static final String PROP_USER = "milvus.connection.user";

  public static final String PROP_PASSWORD = "milvus.connection.password";
  public static final String PROP_TOKEN = "milvus.connection.token";
  public static final String PROP_DB_NAME = "milvus.connection.dbName";

  public static ConnectConfig.ConnectConfigBuilder createConnectionBuilder(
      String host, Properties properties) {
    ConnectConfig.ConnectConfigBuilder builder = ConnectConfig.builder();
    String uri = null;
    if (host != null) {
      uri = String.format("http://%s");
      builder.uri(uri);
    }
    if (properties != null && properties.size() > 0) {
      if (uri == null) {
        uri = properties.getProperty(PROP_URI);
        Validate.notEmpty(uri, "uri is empty!");
        builder.uri(uri);
      }
      fillProps(builder, properties);
    }
    return builder;
  }

  protected static void fillProps(ConnectConfig.ConnectConfigBuilder builder,
      Properties properties) {
    String v = properties.getProperty(PROP_USER);
    if (v != null) {
      builder.username(v);
      builder.password(properties.getProperty(PROP_PASSWORD));
    }
    v = properties.getProperty(PROP_DB_NAME);
    if (v != null) {
      builder.dbName(v);
    }
    v = properties.getProperty(PROP_IDLE_TIMEOUT);
    if (v != null) {
      builder.idleTimeoutMs(Long.valueOf(v) * 1000);
    }
    v = properties.getProperty(PROP_CONNECTION_TIMEOUT);
    if (v != null) {
      builder.connectTimeoutMs(Long.valueOf(v) * 1000);
    }
    v = properties.getProperty(PROP_KEEPALIVE_TIMEOUT);
    if (v != null) {
      builder.keepAliveTimeMs(Long.valueOf(v) * 1000);
    }
  }
}
