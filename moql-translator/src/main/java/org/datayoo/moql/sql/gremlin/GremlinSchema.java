package org.datayoo.moql.sql.gremlin;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author tangtadin
 * @version 1.0
 * @description: TODO
 * @date 2023/2/11 23:25
 */
public abstract class GremlinSchema {

  public static final String ENTITY_VERTEX = "v";

  public static final String ENTITY_EDGE = "e";

  public static final String ENTITY_PATH = "p";

  public static final Set<String> vertexFields = new HashSet();


}
