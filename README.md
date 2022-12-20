# data-warehouse-backend
> 数据仓库期末项目后端，采用flask框架，运行端口5000
>
> master分支为整合版，真实部署时将mysql&neo4j和spark分别部署于两台服务器上，通过切换分支可以查看相应版本
> 
## 运行后端

- 后台运行
```shell
$ nohup python main.py &
```
- 终止
```shell
$ ps -aux|grep -v grep |grep main.py |awk '{print $2}'| xargs kill -9
```
- using screen will be more elegant
```shell
# install
$ sudo apt install screen # ubuntu

# enter new screen
$ screen -R flask

# in new screen
$ python main.py

# leave
$ ctrl+a+d

# kill
$ screen -X -R flask quit
```

## 数据库配置

- 三个数据库均用docker配置，mysql和neo4j的配置较为简单，hadoop+hive+spark的配置较为复杂，具体可以参考[mysql&neo4j](www.baidu.com)和[hadoop+hive+spark]()。
- 由于spark的内存消耗很大，我们将spark和负责查询spark的后端分开部署在两台服务器上。

## 框架结构

- mysql\_, neo4j\_, spark\_三个文件夹分别绑定对应数据库的操作，使用下划线是因为neo4j与python本身的库冲突

  - \*\*/api/comprehensive.py用于综合查询

  - mysql\_/api/count.py用于分页查询总数

  - mysql\_/suggest.py用于查询搜索建议

  - 其余文件如bytime.py用于最初测试api查询时所用，为单独查询，在最后版本中没有被使用到

- 运行时首先让运行配置的数据库，再进入根目录运行main.py即可
- 在main.py中注册了相关api的蓝图，开启了跨域，并写了两个测试接口

## api说明

- mysql, spark提供了电影信息的综合查询，可以自选筛选条件字段和电影信息字段，即

  - 提供的筛选条件字段：

  |字段名|变量|说明|
  |--------|---------------------------------|------|
  |上映日期|year,month,day,season,weekday|int|
  |导演|director|string|
  |演员|actor|string|
  |风格|genre_name|string|
  |评分|min_score,max_score|float|

    - 提供的电影信息字段

    |字段名|变量|说明|
    |--------|---------------------------------|------|
    |电影标题|title|string|
    |电影版本数|edition|string|
    |电影版本|format|string|
    |电影风格|genre_name|string|
    |电影评分|score|float|
    |上映日期|date|date|
    |导演|directors|[string]|
    |演员|actors|[string]|

  上述，筛选字段可空，若为空则不作为筛选条件；电影信息字段也可空，若空则不会去查询相关的表格，减少查询时间。

- mysql, neo4j, spark提供了对演员和演员、导演和演员关系的查询，即选定某个演员或导演，查询与其经常合作的演员或导演的姓名。

  |字段名|变量|说明|
  |---|---|---|
  |查询源身份|source|enum<actor, director>|
  |查询目标身份|target|enum<actor, director>|
  |姓名|name|string|
  |最少合作次数|times|int|

- 其余一些api为初期测试所用，再此不再过多赘述
