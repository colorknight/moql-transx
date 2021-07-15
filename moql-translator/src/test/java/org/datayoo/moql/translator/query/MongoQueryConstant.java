package org.datayoo.moql.translator.query;

/**
 * @author BLADE
 * @version 1.0
 * @className MongoQueryConstant
 * @description TODO
 * @date 7/3/2021 2:18 AM
 **/
public class MongoQueryConstant {

  /**
   * 默认handler
   */
  public static final String DEFAULT_HANDLER = "defaultHandler";

  public static final String DB = "db";
  /**
   * 查询类型
   */
  public static final String QUERY_TYPE_FIND = "find";
  public static final String QUERY_TYPE_AGGREGATE = "aggregate";

  /**
   * 查询类型
   */
  public static final String QUERY_TYPE = "queryType";
  /**
   * 查询集合名称
   */
  public static final String QUERY_COLLECTION = "queryCollection";
  /**
   * 返回字段
   */
  public static final String PROJECT = "$project";

  public static final String MATCH = "$match";

  public static final String AND = "$and";

  public static final String OR = "$or";

  public static final String AGGREGATE = "aggs";

  public static final String GROUP = "$group";

  public static final String LIMIT = "$limit";
  public static final String SKIP = "$skip";

  public static final String COUNT = "$count";

}
