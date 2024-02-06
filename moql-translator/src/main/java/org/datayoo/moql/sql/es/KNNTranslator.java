package org.datayoo.moql.sql.es;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.datayoo.moql.Operand;
import org.datayoo.moql.operand.function.Function;

import java.util.List;

public class KNNTranslator extends AbstractESFunctionTranslator {

  public static final String FUNCTION_NAME = "knn";

  public KNNTranslator() {
    super(FUNCTION_NAME);
  }

  @Override
  protected void innerTranslate(Function function, JsonElement jsonObject) {
    // TODO Auto-generated method stub
    if (function.getParameterCount() != 4) {
      throw new IllegalArgumentException(
          "Error function! The match function's format should be match(fields,query_vector,k,num_candidates)!");
    }
    JsonObject knn = new JsonObject();

    List<Operand> parameters = function.getParameters();

    putObject(knn, "field", trimChar(parameters.get(0).toString(), "'"));
    String vector = trimChar(parameters.get(1).toString(), "'");
    vector = trimChar(vector, "[");
    vector = trimChar(vector, "]");
    String[] vectors = StringUtils.split(vector, ",");
    double[] queryVector = new double[vectors.length];
    for (int i =0; i < vectors.length; i ++) {
      queryVector[i] = Double.parseDouble(vectors[i]);
    }
    JsonArray jsonArray = new Gson().toJsonTree(queryVector).getAsJsonArray();
    knn.add("query_vector", jsonArray);
    putObject(knn, "k", parameters.get(2));
    putObject(knn, "num_candidates", parameters.get(3));

    putObject(jsonObject, "knn", knn);
  }

  private String trimChar(String str, String replaceChar) {
    if (str.startsWith(replaceChar)) {
      str = str.substring(1);
    }
    if (str.endsWith(replaceChar)) {
      str = str.substring(0, str.length() -1);
    }
    return str;
  }
}
