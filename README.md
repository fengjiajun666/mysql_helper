# mysql_helper

一个封装了 MySQL 增删改查操作的 Python 工具类。

## 环境要求

- Python 3.8+
- MySQL 8.0+（必须安装并启动）

## 运行步骤

### 1. 安装依赖

```bash
pip install -r requirements.txt
```
### 2. 修改路径

复制 config.example.json 为 config.json（或直接重命名），并将 password 改为您的 MySQL 密码。

### 3. 启动mysql

确保 MySQL 服务已启动。

### 4. 封装代码在helper中，运行demo代码可以验证增删改查功能。

运行后会在mysql中生成名为testdb的数据库，并会进行增删改查演示。
