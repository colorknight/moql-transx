​	众所周知，邮件、图片、音频、视频等非结构化数据已占据了我们日常生活数据总量的80%以上。如果想要使用计算机来处理这些数据，需要使用embedding技术将它们转化为向量。Milvus作为一款开源的向量数据库，可存储这些向量，并提供基于向量的索引及检索服务。Milvus提供了一套类SQL的数据检索API，方便使用者通过这些API检索数据。由于它与SQL语法间仍有差别，且必须通过编程的方式才能获取数据，使用起来仍不够方便。

​    为了能让使用者以类似访问关系数据库的交互体验访问Milvus向量数据库。MOQL Transx继续秉承能SQL化检索数据库就SQL化检索数据库的宗旨。为用户提供了一套可以检索Milvus向量数据库的SQL语法，并提供了检索接口。使用者可通过该接口输入SQL语句，获得结构化的数据结果，如下列代码示例：

```
// 构建Milvus客户端
MilvusServiceClient milvusClient = new MilvusServiceClient(ConnectParam.newBuilder()
						.withHost("172.31.179.128")
            .withPort(19530)
            .build();
// 使用Milvus客户端创建Milvus查询器
MilvusQuerier milvusQuerier = new MilvusQuerier(milvusClient);
/* 查询语句含义：从book集合中筛选数据，并返回col1,col2两个列。筛选条件为，当数据的col3列值为4，col4列值为'a','b','c'中的任意一
 个，且vec向量字段采用'L2'类型匹配，值为'[[1.0, 2.0, 3.0],[1.1,2.1,3.1]]'。另外，采用强一致性级别在10个单元内进行检索，取第11到第15，5条命中记录。*/
String sql = "select col1, col2 from book where col3 = 4 and vMatch(vec, 'L2', '[[1.0, 2.0, 3.0],[1.1,2.1,3.1]]') and col4 in ('a', 'b', 'c') and consistencyLevel('STRONG') and nProbe(10) limit 10,5";
// 使用查询器执行sql语句，并返回查询结果
RecordSet recordSet = milvusQuerier.query(sql);
```

​	也可以使用MilvusQuerier将SQL语句翻译为SearchParam，然后调用MilvusServiceClient.search方法获取查询结果，如下列代码所示：

```
MilvusQuerier milvusQuerier = new MilvusQuerier();
String sql = "select col1, col2 from book where col3 = 4 and vMatch(vec, 'L2', '[[1.0, 2.0, 3.0],[1.1,2.1,3.1]]') and col4 in ('a', 'b', 'c') and consistencyLevel('STRONG') and nProbe(10) limit 10,5";
// 将SQL语句翻译为SearchParam
SearchParam searchParam = milvusQuerier.buildSearchParam(sql);
// 调用MilvusServiceClient.search接口获得查询结果
R<SearchResults> respSearch = milvusClient.search(searchParam);
```

​	Milvus提供的检索接口与SQL语法有一定差异，其SearchParam提供的部分参数可以直接映射为SQL语法的等同语义子句。如：expr参数，其语义与SQL中Where子句语义基本兼容；其OutFields参数为输出结果集的列结构，与SQL语句的Select子句语义相同。但其也有其特殊的查询参数接口，如：针对向量字段匹配的参数接口withVectors、withVectorFieldName；表示匹配一致性级别的withConsistencyLevel接口等。由于这些概念在SQL中没有对应语义的子句，为不增加语法概念，MOQL Transx将这类接口都以Where子句中的函数形式进行表达。这种表达方式可能不是最佳表达方式，如果有人有更好的建议，可以到项目中给我们留言。

​	下表将给出Milvus查询接口的参数与SQL语法的对照关系：

| Milvus查询参数接口                             | SQL语法                                               |
| ---------------------------------------------- | ----------------------------------------------------- |
| withCollectionName(table)                      | from table                                            |
| withOutFields(outFields)                       | select outFields                                      |
| withExpr(expr)                                 | where expr                                            |
| withVectorFieldName,withMetricType,withVectors | vMatch(fieldName, metricType, vectors)                |
| withConsistencyLevel                           | consistencyLevel(['STRONG'\|'BOUNDED'\|'Eventually']) |
| withTopK(k)                                    | limit offset, k                                       |
| withParams:nProbe                              | nProbe(long)                                          |
| withParams:ef                                  | ef(long)                                              |
| withParams:search_k                            | searchK(long)                                         |
| withPartitionNames                             | partitionBy(String[])                                 |
| withRoundDecimal                               | roundDecimal(int)                                     |
| withTravelTimestamp                            | travelTimestamp(long)                                 |

​	模块的maven坐标

```
        <dependency>
            <groupId>org.datayoo.moql</groupId>
            <artifactId>moql-querier-milvus</artifactId>
            <version>1.0.0</version>
        </dependency>
```