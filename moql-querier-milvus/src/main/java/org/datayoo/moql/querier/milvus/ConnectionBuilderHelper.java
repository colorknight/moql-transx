package org.datayoo.moql.querier.milvus;

import io.milvus.param.ConnectParam;
import org.apache.commons.lang3.Validate;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author tangtadin
 * @version 1.0
 * @description: TODO
 * @date 2023/2/23 12:40
 */
public abstract class ConnectionBuilderHelper {

  public static final String PROP_PORT = "milvus.connection.port";
  public static final String PROP_URI = "milvus.connection.uri";
  public static final String PROP_IDLE_TIMEOUT = "milvus.connection.idleTimeout";

  public static final String PROP_CONNECTION_TIMEOUT = "milvus.connection.connectionTimeout";

  public static final String PROP_KEEPALIVE_TIMEOUT = "milvus.connection.keepaliveTimeout";

  public static final String PROP_USER = "milvus.connection.user";

  public static final String PROP_PASSWORD = "milvus.connection.password";

  public static ConnectParam.Builder createConnectionBuilder(String host,
      Properties properties) {
    ConnectParam.Builder builder = ConnectParam.newBuilder();
    if (properties != null && properties.size() > 0) {
      String uri = properties.getProperty(PROP_URI);
      if (uri == null) {
        Validate.notEmpty(host, "host is empty!");
        builder.withHost(host);
        String port = properties.getProperty(PROP_PORT);
        if (port != null) {
          builder.withPort(Integer.valueOf(port));
        }
      } else {
        builder.withUri(uri);
      }
      fillProps(builder, properties);
    } else {
      Validate.notEmpty(host, "host is empty!");
      builder.withHost(host);
    }
    return builder;
  }

  protected static void fillProps(ConnectParam.Builder builder,
      Properties properties) {
    String v = properties.getProperty(PROP_USER);
    if (v != null) {
      builder.withAuthorization(v, properties.getProperty(PROP_PASSWORD));
    }
    v = properties.getProperty(PROP_IDLE_TIMEOUT);
    if (v != null) {
      builder.withIdleTimeout(Long.valueOf(v), TimeUnit.SECONDS);
    }
    v = properties.getProperty(PROP_CONNECTION_TIMEOUT);
    if (v != null) {
      builder.withConnectTimeout(Long.valueOf(v), TimeUnit.SECONDS);
    }
    v = properties.getProperty(PROP_KEEPALIVE_TIMEOUT);
    if (v != null) {
      builder.withKeepAliveTimeout(Long.valueOf(v), TimeUnit.SECONDS);
    }
  }
}
