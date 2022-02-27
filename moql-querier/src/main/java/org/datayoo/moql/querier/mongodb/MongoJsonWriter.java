package org.datayoo.moql.querier.mongodb;

import java.io.StringWriter;
import java.util.regex.Pattern;

/**
 * @author tangtadin
 * @version 1.0
 * @description: TODO
 * @date 2022/2/27 4:33 PM
 */
public class MongoJsonWriter extends StringWriter {

  protected static Pattern funcPattern = Pattern.compile(".*\\([^\\)]*\\)");

  protected String funcStr = null;

  protected boolean funcEnd = false;

  @Override
  public void write(String str) {
    if (funcEnd) {
      funcEnd = false;
      return;
    }
    if (funcStr == null) {
      if (str.length() > 2 && str.charAt(str.length() - 1) == ')') {
        getBuffer().deleteCharAt(getBuffer().length() - 1);
        funcEnd = true;
      }
    }
    if (funcStr != null && str.length() == 2 && str.equals("\\\"")) {
      str = "'";
    }
    super.write(str);
  }

  @Override
  public void write(String str, int off, int len) {
    if (funcEnd) {
      funcEnd = false;
      return;
    }
    if (funcStr == null) {
      if (str.length() > 2 && str.charAt(str.length() - 1) == ')') {
        getBuffer().deleteCharAt(getBuffer().length() - 1);
        funcStr = str;
      }
    } else if (funcStr == str) {
      if (str.length() == off + len) {
        funcStr = null;
        funcEnd = true;
      }
    }
    if (funcStr != null && str.length() == 2 && str.equals("\\\"")) {
      str = "'";
    }
    super.write(str, off, len);
  }

}
