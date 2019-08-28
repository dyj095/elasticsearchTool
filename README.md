# elasticsearch工具
## 一、 主要功能
--------------------------ES工具程序命令说明---------------------------<br>
export: 导出指定es的数据，文件格式为json;<br>命令格式：export [http://ip:port/index_name/type_name] [output path] [type: 0: 单文件	1:output path目录下第1000条一个文件]<br>
import: 将json数据文件导入到指定的es中;  <br>命令格式：import [http://ip:9200/index_name/type_name] [input path] [idKeyName] [type 0:文件 1:目录下的所有文件]<br>
createIndex:创建索引;<br>命令格式：createIndex [http://ip:9200/index_name]<br>
createMapping:创建索引;<br>命令格式：createMapping [http://ip:9200/index_name/type_name] [input path]<br>
quit: 退出<br>
---------------------------------------------------------------------<br>
## 二、 运行方式
### 1、以可运行的jar执行命令
 用eclipse将工程打成可运行的jar<br>
 执行命令
> ```shell
> java -jar elasticsearchTool.jar
> // 如果数据不大（没有超过1.5M）,采用以单文件方式导出数据
> export http://192.168.2.2:9200/elink/chatmessage d:/chatmessage.json 0
> // 如果数据大（没有超过1.5M）,采用以多文件方式导出数据到指定的目录
> export http://192.168.2.2:9200/elink/chatmessage d:/chatmessage 1
> ```
