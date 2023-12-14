​	众所周知，邮件、图片、音频、视频等非结构化数据已占据了我们日常生活数据总量的80%以上。如果想要使用计算机来处理这些数据，需要使用embedding技术将它们转化为向量。腾讯云向量数据库（Tencent Cloud VectorDB）是一款全托管的自研企业级分布式数据库服务，专用于存储、检索、分析多维向量数据，可存储这些向量，并提供基于向量的索引及检索服务。Tencent Cloud VectorDB提供了一套类SQL的数据检索API，方便使用者通过这些API检索数据。由于它与SQL语法间仍有差别，且必须通过编程的方式才能获取数据，使用起来仍不够方便。

​    为了能让使用者以类似访问关系数据库的交互体验访问腾讯云向量数据库。MOQL Transx继续秉承能SQL化检索数据库就SQL化检索数据库的宗旨。为用户提供了一套可以检索腾讯云向量数据库的SQL语法，并提供了检索接口。使用者可通过该接口输入SQL语句，获得结构化的数据结果，如下列代码示例：

```
// 构建TcVector客户端
ConnectParam connectParam = ConnectParam.newBuilder().withUrl("向量数据库url")
        .withUsername("root").withKey("访问key").withTimeout(30).build();
vectorDBClient = new VectorDBClient(connectParam,

// 使用TcVector客户端创建TcVector查询器
TcVectorQuerier querier = new TcVectorQuerier(vectorDBClient);
/* 查询语句含义：从book集合中筛选数据，并返回全部列。筛选条件为，向量字段值为'[[0.3123, 0.43, 0.213], [0.5123, 0.63, 0.413]]'。取前2条命中记录。*/
String sql = "select * from datayoo.book where withVectors('[[0.3123, 0.43, 0.213], [0.5123, 0.63, 0.413]]') limit 2";
// 使用查询器执行sql语句，并返回查询结果
RecordSet recordSet = querier.query(sql);
```


​	TcVector提供的检索接口与SQL语法有一定差异，其SearchParam提供的部分参数可以直接映射为SQL语法的等同语义子句。如：expr参数，其语义与SQL中Where子句语义基本兼容；其OutFields参数为输出结果集的列结构，与SQL语句的Select子句语义相同。但其也有其特殊的查询参数接口，如：针对向量字段匹配的参数接口withVectors、withVectorFieldName；表示匹配一致性级别的withConsistencyLevel接口等。由于这些概念在SQL中没有对应语义的子句，为不增加语法概念，MOQL Transx将这类接口都以Where子句中的函数形式进行表达。这种表达方式可能不是最佳表达方式，如果有人有更好的建议，可以到项目中给我们留言。

​	下表将给出TcVector查询接口的参数与SQL语法的对照关系：

| TcVector查询参数接口                                 | SQL语法        |
|------------------------------------------------|--------------|
| withParams                                     | where expr   |
| withVectors                                    | where expr  |
| withDocumentIds                                    | where expr  |
| withFilter                                    | where expr  |
| collection(table)                      | from table   |
| withOutputFields(outFields)                       | select outFields |
| withExpr(expr)                                 | where expr   |
| withLimit()                       | limit offset, k |

​	模块的maven坐标

```
        <dependency>
            <groupId>org.datayoo.moql</groupId>
            <artifactId>moql-querier-tcvector</artifactId>
            <version>1.0.0</version>
        </dependency>
```