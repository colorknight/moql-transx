package org.datayoo.moql.sql.es;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
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

    putObject(knn, "field", parameters.get(0));
    putObject(knn, "query_vector", parameters.get(1));
    putObject(knn, "k", parameters.get(2));
    putObject(knn, "num_candidates", parameters.get(3));

    putObject(jsonObject, "knn", knn);
  }
}
