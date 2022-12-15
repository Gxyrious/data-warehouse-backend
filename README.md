# data-warehouse-backend
数据仓库期末项目后端
## 运行
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
sudo apt install screen # ubuntu

# enter new screen
screen -R flask

# in new screen
python main.py

# leave
ctrl+a+d

# kill
screen -X -R flask quit
```