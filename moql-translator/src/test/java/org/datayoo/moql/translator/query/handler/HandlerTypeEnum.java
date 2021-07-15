package org.datayoo.moql.translator.query.handler;

import org.datayoo.moql.translator.query.MongoQueryConstant;
import org.datayoo.moql.translator.query.handler.*;

/**
 * @author BLADE
 * @version 1.0
 * @enumName HandlerTypeEnum
 * @description TODO
 * @date 7/8/2021 6:26 PM
 **/
public enum HandlerTypeEnum {
  ROOT("root", RootHandler.class), QUERY_COLLECTION("queryCollection",
      QueryCollectionHandler.class), PROJECT("$project",
      ProjectQueryHandler.class), QUERY_TYPE("queryType",
      QueryTypeHandler.class), MATCH("$match", MatchHandler.class), AND("$and",
      AndHandler.class), OR("$or", OrHandler.class), AGGREGATE(
      MongoQueryConstant.AGGREGATE, AggregateHandler.class), group(
      MongoQueryConstant.GROUP, GroupHandler.class), LIMIT(
      MongoQueryConstant.LIMIT, LimitHandler.class), SKIP(
      MongoQueryConstant.SKIP, SkipHandler.class), COUNT(
      MongoQueryConstant.COUNT, CountHandler.class), DEFAULT(
      MongoQueryConstant.DEFAULT_HANDLER, DefaultHandler.class);

  private String type;
  private Class<?> clazz;

  HandlerTypeEnum(String type, Class<?> clazz) {
    this.type = type;
    this.clazz = clazz;
  }

  public String getType() {
    return type;
  }

  public Class<?> getClazz() {
    return clazz;
  }
}
