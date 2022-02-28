## MOQL-Transx简介

​	MOQL-Transx是MOQL工程的姊妹工程，其早期与MOQL工程在一起，后独立出来。其核心思想是想让数据库使用者只掌握一种语言，即SQL语言，就可以去查询访问各类不同的数据库，以此减小开发者，使用者的学习成本。由于各类数据库语法差异比较大，MOQL-Transx并不追求从SQL语义到各类数据库DSL语义的完整映射，更多是将最常用到的语法功能进行了映射。目前MOQL-Transx已经完成了从标准的MOQL语法(SQL92语法的子集)到MySQL、Oracle、DB2、SQLServer、Postgresql等关系数据库间的语法转换(注：这些关系数据库间的语法基本相同，有微小的差异)。同时，也完成了MOQL语法到ElasticSearch DSL，MongoDB DSL及DynamoDB DSL的语法转换。由于ElasticSearch，MongoDB都属于文档型数据库，其返回结构非我们常用的关系型数据库的二维表格，故MOQL-Transx还进一步提供了这两个数据库的查询器。这些查询器可以接受一个sql作为参数，并返回一个二维表结构的对象，使开发者使用起来更直观方便。

​	MOQL-Transx内部包括两个模块，moql-translator用于完成从MOQL语法到各类数据库DSL语法的转换；moql-querier用于为特殊的数据库提供可以接受sql语句返回二维表结构的查询器。

## SQL to ElasticSearch DSL

​	众所周知ElasticSearch目前是一个应用最为广泛的分布式搜索与分析引擎，它的功能强大，能够已很高的性能访问大规模数据。它拥有强大的查询分析语法，能够完成模糊查询、精准查询及聚集计算等诸多功能的表达。但对于那些用惯了SQL语言的数据分析人员来说，掌握ElasticSearch的DSL语言来做以前熟悉的事情，还是有比较陡的学习曲线的。

​	MOQL转换器用于完成从MOQL语法(SQL语法的一个子集，涵盖了绝大多数常用语法)到各类不同数据库SQL语法的转换，如它提供到Oracle、SqlServer以及DB2等不同SQL方言的转换，也提供了到ElasticSearch DSL的语法转换。有了这种转换，可以大大降低数据分析人员学习不同分析语言的学习曲线，能够快速的享受新的分析引擎技术带来的便利。

​	MOQL本质上是一个开发程序包，应用它进行SQL到ElasticSearch DSL的转换非常方便，如下代码示例：

```
String sql = "select ip.src, max(ip.sport), min(ip.sport) from ip3 ip group by ip.src order by ip.src desc limit 10 ";
 try {							
    String es = MoqlTranslator
        .translateMoql2Dialect(sql, SqlDialectType.ELASTICSEARCH);
    es = es.trim();
    System.out.println(es);
  } catch (MoqlException e) {
    e.printStackTrace();
  }
}
```

该代码执行完转换后，输出的ElasticSearch DSL语法为：

```
{	
  "size": 10,
  "sort": [
    {	
      "src": {
        "order": "desc"
      }
    }
  ],
  "query": {	
    "match_all": {}
  },	
  "aggs": {
    "src": {
      "terms": {
        "field": "src"
      },
      "aggs": {
        "max$ip_sport$": {// 程序自动起的别名
          "max": {
            "field": "sport"
          }
        },
        "min$ip_sport$": {// 程序自动起的别名
          "min": {
            "field": "sport"
          }
        }
      }
    }
  }
}
```

​	将该结果提交给ElasticSearch引擎可以达到与程序中sql语句匹配的执行效果。就是这样简单，可以立刻去<https://github.com/colorknight/moql>下载个源码试试了。

​	当然由于ElasticSearchDSL语法的能力过于强大，MOQL目前还无法提供完整的转换能力，并且为适应两种不同语法间的差异还做了些技巧性的设计，需要使用者在使用时加以注意。

​	ElsaticSearch DSL的query子句中有query上下文与filter上下文两种上下文(本文不解释两种上下文的差异，请参见相关资料)，这两种上下文分别对应了SQL语法的两个子句。query上下文对应了where子句，filter上下文对应了having子句。这样的对应关系主要是技巧性的对应，并不体现各部分在ElasticSerach上的执行顺序。所以在应用ElasticSearch DSL的此类特性时需要留意，不要写错SQL子句。

​	另外，对于MOQL现在不能支持的ElasticSearch DSL的特殊语法，MOQL建议通过编写函数(UDF,User Define Function)的方式予以扩展。扩展时通过继承org.moql.sql.es.ESFunctionTranslator接口来实现扩展函数，然后再去org.moql.sql.es. ElasticSearchTranslator中注册函数即可。

​	下面将给出SQL与ElasticSearch DSL的语法转换对照表，方便使用者全面了解SQL转换成ElasticSearch DSL后能达到的语法能力。

| MOQL                                  | ElasticSearch DSL                        |
| ------------------------------------- | ---------------------------------------- |
| UNION,INTERSECT,EXCEPT等集合操作子句         | 未转换映射                                    |
| SELECT子句                              | 当MOQL语句中不含DISTINCT和GROUP子句时，映射为ElasticSearch  DSL的_source子句。若SQL语法的select子句非”.*”模式时，即有具体的投映列时，这些列字段将被放入_source子句的includes属性中，否则则忽略_source子句;而当MOQL含有DISTINCT和GROUP子句时，映射为ElasticSearch的Aggs。此时SELECT子句中的投影列需遵循SQL语法的约定，这样才能正确转换。 |
| DISTINCT子句                            | 转换为ElasticSearch的Aggs子句。                 |
| FROM子句                                | 不进行转换                                    |
| WHERE子句,HAVING子句                      | WHERE子句映射为query子句的query上下文，HAVING子句映射为query子句的filter上下文 |
| and                                   | 在WHERE子句中时被转换为must;在HAVING子句中时被转换为filter |
| or                                    | 转换为should                                |
| not                                   | 转换为must_not                              |
| <>(不等于)                               | 转换为must_not+term                         |
| =(等于)                                 | 转换为term                                  |
| >(大于)、<(小于)、>=(大于等于)、<=(小于等于)、between | 转换为range                                 |
| like                                  | 转换为regexp                                |
| in                                    | 转换为terms                                 |
| is                                    | 转换为must_not+exists                       |
| 用于改变优先级的括号                            | 转换为层级关系                                  |
| GROUP BY                              | 转换为ElasticSearch的Aggs子句                  |
| LIMIT子句                               | 当MOQL语句中不含有DISTINCT和GROUP子句时，该子句被转换为ElasticSearch  DSL的from和size属性；而当含有这两个子句时，该值会被映射到terms aggregation子句的size 属性中。 |
| ORDER子句                               | 当MOQL语句中不含有DISTINCT和GROUP子句时，该子句被转换为ElasticSearch  DSL的sort子句；而当含有这两个子句时，该值会被映射到terms aggregation子句的order 子句中。 |

​	MOQL用扩展函数的方式来支持ElasticSearch DSL语法中SQL无法描述的部分，如下：

| ElasticSearch DSL   | MOQL                                     |
| ------------------- | ---------------------------------------- |
| match               | match(fields,queryString)  fields：字符串数组，表示多个字段时，字段间用“,”隔开。如：‘field1,field2’。当只有一个field值时，该函数被映射为match子句，当fields字段有多个值时，该函数被映射为malti_match子句  queryString：字符串，表示检索条件，等同与match与multi_match中的query属性 |
| multi_match         | 见match                                   |
| match_phrase        | matchPhrase(field,  queryString)或matchPhrase(field, queryString, analyzer)  field：字符串，match_phrase子句的名字  queryString：字符串，表示match_phrase子句的query属性  analyzer：字符串，与match_phrase的同名属性一致 |
| match_phrase_prefix | matchPhrasePrefix(field,  queryString)或matchPhrasePrefix(field, queryString, analyzer)  field：字符串，match_phrase_prefix子句的名字  queryString：字符串，表示match_phrase_prefix子句的query属性  analyzer：字符串，与match_phrase的同名属性一致 |
| terms_set           | termsSet(field,  valueSet, minMatchField)  field：字符串，terms_set子句的名字  valueSet：字符串数组，表示terms数组，当需要输入多个值时，值与值之间用”,”隔开。  minMatchField：字符串，表示minimum_should_match_field属性 |
| regex               | regex(field,pattern)  field：字符串，字段名  pattern：字符串，表示正则表达式的模式。 |
| fuzzy               | fuzzy(field,  value)  或fuzzy(field,value,fuzziness,prefix_length,max_expansions)  field：字符串，字段名  value：字符串，字段值  fuzziness：整数，与fuzzy子句同名属性一致  prefix_length：整数，与fuzzy子句同名属性一致  prefix_length：整数，与fuzzy子句同名属性一致 |
| type                | type(value)  value：字符串，字段值               |
| ids                 | ids(type,  values)  type：字符串，与ids子句同名属性一致。  values：字符串数组，多个值之间用”,”隔开。与ids子句同名属性一致。 |
| more_like_this      | moreLike(fields,likeText,minTermFreq,maxQueryTerms)  fields：字符串数组，表示多个字段时，字段间用“,”隔开。表示more_like_this的fields属性  likeText：字符串，表示表示more_like_this的like_text属性  minTermFreq：整数，表示more_like_this的min_term_freq属性  maxQueryTerms：整数，表示more_like_this的max_query_terms属性 |

## SQL to ElasticSearch DSL改进

​	最近团队在使用MOQL的SQL到ElasticSearch DSL转换时提出，该转换器不能完成深度分页场景的应用。而ElasticSearch为该类应用提供了“search_after”的参数解决方案。ElasticSearch的这个解决方案使得前后两个QUERY DSL有了上下文依赖，后续的查询要依赖上一个查询结果中返回的内容作为条件拼装检索语句。为满足这个需求，MOQL升级了moql-translator和moql-querier两个模块。

 	在moql-translator中整体升级了SqlTranslator接口，允许传入一个Map类型的translationContext参数，该参数可以带入语法转换时所需的参数，这样所有的语法转换器都可以支持有上下文依赖的语法转换了。示例代码如下：

```
// 带有limit的SQL语句，按照search_after的说明，limit语法中的数字20表示from，将被忽略

String sql = "select w.* from web w where w.port=443 limit 20,10";

Map<String, Object> translationContext = new HashMap<String, Object>();

Object[] features = new Object[] { 133, "test" };

// RESULT_SORT_FEATURES常量为传递给search_after语法的参数的名字

translationContext

    .put(EsTranslationContextConstants.RESULT_SORT_FEATURES, features);

testESDialect(sql, translationContext);
```

该代码执行完转换后，输出的ElasticSearch DSL语法为：

```
{
  "search_after": [
    133,
    "test"
  ],
  "size": 10,
  "query": {
    "term": {
      "port": "443"
    }
  }
}
```



​	即然语法转换有了上下文的需求，后续的查询依赖于前序查询的结果，就需要能够从结果中取出后续查询所依赖的上下文信息。之前的moql-querier只能读取结果中的部分数据，并以RecordSet的形式返回。而search_after所依赖的结果中的sort字段无法获得。故此次也对DataQuerier接口进行了升级，为接口中加入了一个SupplementReader参数。用户可以通过实现SupplementReader接口读取返回结果集中的其它信息。MOQL提供了一个CommonSupplementReader的缺省实现，可以读取检索结果中的total，max_score及sort等字段信息。

​	示例代码如下：

```
String sql = "select t.DVC_ADDRESS, t.MESSAGE from ins_test t order by t.SEVERITY LIMIT 5";

try {

  CommonSupplementReader supplementReader = new CommonSupplementReader();

  RecordSet recordSet = dataQuerier.query(sql, supplementReader);

  outputRecordSet(recordSet);

  System.out.println(supplementReader.getTotalHits());

} catch (IOException e) {

  e.printStackTrace();

}
```

​	使用者可以通过访问supplementReader获取结果集中返回的其它结果信息。

## 用SQL访问MongoDB

​	MOQL-Transx提供了一个轻量级的访问MongoDB数据库的解决方案，并不需要安装额外的数据库引擎。与MOQL-Transx提供的用SQL访问ElasticSearch数据库的解决方案一样。该解决方案首先将SQL语法映射为MongoDB的DSL，然后又提供了查询器执行DSL并将查询结果以二维表结构的形式返回。由于MongoDB不像ElasticSearch，其有一个完整的DSL。其语法由多个语法片段组成，这些语法片段需要作为参数传入不同的api才能完成一次完整意义的查询语义。由于我们无法抽象描述这种片段+API调用的带有交互的逻辑，故在做这种语义翻译时，参考MongoDB中，aggregate计算的片段语法结构，设计了一个能够基本完成SQL语义映射的伪MongoDB DSL。使用者可以不必了解该DSL，直接使用moql-querier中提供的MongoDBQuerier直接访问MongoDB数据库。

​	下面是一个使用MongoDBTranslator将SQL转换为MongoDB伪DSL的例子，可以帮助使用者大致了解MOQL-Transx如何设计实现了SQL到MongoDB的语法映射。

```
select w.dns, w.ip from db.web w where (w.port=443 or w.port=8080) and w.ip='127.0.0.1' or w.ip='127.0.0.2' order by w.ip desc limit 2
```

​		上面的SQL语句通过MongoDBTranslator转换为如下格式：

```
[
  {
    "queryType": "find"
  },
  {
    "queryCollection": "db.web"
  },
  {
    "$project": {
      "dns": 1,
      "ip": 1,
      "_id": 0
    }
  },
  {
    "$match": {
      "$or": [
        {
          "$and": [
            {
              "$or": [
                {
                  "port": {
                    "$eq": 443
                  }
                },
                {
                  "port": {
                    "$eq": 8080
                  }
                }
              ]
            },
            {
              "ip": {
                "$eq": "127.0.0.1"
              }
            }
          ]
        },
        {
          "ip": {
            "$eq": "127.0.0.2"
          }
        }
      ]
    }
  },
  {
    "$sort": {
      "ip": -1
    }
  },
  {
    "$limit": 2
  }
]
```

​	queryType标签表示了检索类型，该值只有find和aggregate两个值，分别对应MongoCollection的find和aggregate方法。queryCollection对应需要检索的集合。\$match,\$sort , \$limit标签则是借鉴MongoDb的aggregate语法结构，将其移植到find方法的调用上了。MongoDBQuerier将解释该DSL，将其拆解成多个语法片段，而后调用MongoCollection提供的不同的功能接口，完成文档的检索，并最终将文档转换为二维表形式返回。

​	下表将给出SQL子句到MongoDB DSL的映射，使用者可根据表中所列的子句能力编写SQL访问MongoDB。

| MOQL                                                  | MongoDB DSL                                                  |
| ----------------------------------------------------- | ------------------------------------------------------------ |
| UNION,INTERSECT,EXCEPT等集合操作子句                  | 未转换映射                                                   |
| SELECT子句                                            | 投影子句，映射为$project标签                                 |
| DISTINCT子句                                          | 未支持                                                       |
| FROM子句                                              | queryCollection标签                                          |
| LEFT JOIN子句                                         | 映射为$lookup标签                                            |
| WHERE子句,HAVING子句                                  | 均映射为\$match标签，其中,当存在HAVING子句时，DSL中会存在两个\$match标签，二者位置不同，表示HAVING子句的\$match标签会在GroupBy子句之后 |
| and                                                   | 映射为\$and                                                  |
| or                                                    | 映射为$or                                                    |
| not                                                   | 支持not and语义，映射为$nor                                  |
| <>(不等于)                                            | 映射为\$ne                                                   |
| =(等于)                                               | 映射为\$eq                                                   |
| >(大于)、<(小于)、>=(大于等于)、<=(小于等于)、between | 分别映射为\$lt,\$gt,\$gte,$lte以及 \$gte与\$lt的组合         |
| like                                                  | 映射为\$regex                                                |
| in                                                    | 映射为\$in,其否定语义会被映射为\$nin                         |
| is                                                    | 其肯定语义被映射为\$eq null；否定语义被映射为\$ne null       |
| exists                                                | 映射为\$exists                                               |
| 用于改变优先级的括号                                  | 映射为层级关系                                               |
| GROUP BY                                              | 映射为\$group,支持mongoDB中声明的sum、avg、min、max、push、addToSet、first、last以及count聚合运算 |
| LIMIT子句                                             | 映射为$limit, limit子句的偏移部分被映射为\$skip              |
| ORDER子句                                             | 映射为$sort                                                  |

​	由于MongoDB实际没有DSL，所以MOQL-Transx提供的这层转换不具有通用性，故MOQL-Transx提供了MongoDBQuerier来提供更加通用的用法。使用方法如下，非常简单：

```
MongoDBQuerier dataQuerier = new MongoDBQuerier();
String[] serverIps = new String[] { "172.30.30.8" };
Properties properties = new Properties();
dataQuerier.connect(serverIps, properties);
RecordSet recordSet = dataQuerier.query("select w.dns, w.ip from mydb.web w where (w.port=443 or w.port=8080) and w.ip='127.0.0.1' or w.ip='127.0.0.2'");
outputRecordSet(RecordSet recordSet)
```

