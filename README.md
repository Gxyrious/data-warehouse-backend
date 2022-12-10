# data-warehouse-backend
数据仓库期末项目后端
## 运行
- 后台运行
```shell
$ nohup python main.py &
```
- 终止(更加优雅)
```shell
kill $(ps aux | grep '[python.py' | awk '{print $2}')
```