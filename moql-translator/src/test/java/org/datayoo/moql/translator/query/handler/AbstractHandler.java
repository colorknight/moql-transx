package org.datayoo.moql.translator.query.handler;

import com.google.gson.JsonElement;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author BLADE
 * @version 1.0
 * @className Handler
 * @description TODO
 * @date 7/3/2021 2:03 AM
 **/
public abstract class AbstractHandler<T extends JsonElement, S>
    implements Handler<T> {

  public static final Logger logger = LoggerFactory
      .getLogger(AbstractHandler.class);

  @Override
  public String handle(T dslPart, Map<String, Object> context) {
    S innerData = separateInnerPart(dslPart, context);
    String selfSql = doHandle(dslPart, context);
    String innerSql = assembleInnerData(innerData, context);
    return assemble(selfSql, innerSql);
  }

  /**
   * 拆分内部嵌套操作
   *
   * @param dslPart
   * @param context
   * @return
   */
  protected abstract S separateInnerPart(T dslPart,
      Map<String, Object> context);

  /**
   * 解析dsl，获取mongo语句
   *
   * @return
   */
  protected abstract String doHandle(T dslPart, Map<String, Object> context);

  /**
   * 组装嵌套操作的语句
   *
   * @return
   */
  protected abstract String assembleInnerData(S jsonElement,
      Map<String, Object> context);

  private String assemble(String selfSql, String innerSql) {
    String assembleSql = "";
    if (StringUtils.isNotEmpty(selfSql)) {
      if (selfSql.contains("%s")) {
        if (StringUtils.isNotEmpty(innerSql)) {
          assembleSql = String.format(selfSql, innerSql);
        }
      } else if (StringUtils.isNoneEmpty(innerSql)) {
        assembleSql = selfSql + innerSql;
      } else {
        assembleSql = selfSql;
      }
    } else if (StringUtils.isNoneEmpty(innerSql)) {
      assembleSql = innerSql;
    }
    return assembleSql;
  }

}
