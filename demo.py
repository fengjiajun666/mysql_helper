"""
MySQL Helper 演示程序
无论 MySQL 里有没有 testdb，运行后都会自动创建
"""

from mysql_helper import MySQLHelper
import json
import os


def load_config():
    """
    从 config.json 文件中加载数据库连接配置
    """
    config_path = "config.json"

    if not os.path.exists(config_path):
        print("=" * 50)
        print("❌ 错误：找不到 config.json 文件！")
        print("=" * 50)
        print("\n请按以下步骤操作：")
        print("1. 复制 config.example.json 文件")
        print("2. 将副本重命名为 config.json")
        print("3. 打开 config.json，把 password 改成你的 MySQL 密码")
        print("=" * 50)
        exit(1)

    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    return config['mysql']


def ensure_database_exists(host, user, password, port, target_db):
    """
    确保目标数据库存在
    如果不存在，就自动创建
    """
    temp_helper = MySQLHelper(
        host=host,
        user=user,
        password=password,
        database="",  # 先不指定数据库
        port=port
    )

    # 创建数据库（IF NOT EXISTS 表示：如果已存在就不再创建）
    temp_helper.execute_non_query(f"CREATE DATABASE IF NOT EXISTS {target_db}")
    print(f"   ✅ 数据库 '{target_db}' 已就绪（不存在则自动创建）")

def demo():

    print("\n" + "=" * 50)
    print("MySQL Helper 功能演示")
    print("=" * 50 + "\n")

    # 加载配置
    mysql_config = load_config()
    target_db = mysql_config['database']

    # 如果配置文件中 database 是空的，使用默认值 testdb
    if not target_db:
        target_db = "testdb"
        print(f"  未指定数据库，使用默认值: {target_db}")
    print(f"   主机: {mysql_config['host']}")
    print(f"   端口: {mysql_config['port']}")
    print(f"   用户: {mysql_config['user']}")
    print(f"   目标数据库: {target_db}")
    print("   (密码已隐藏)")

    # 确保数据库存在
    ensure_database_exists(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        port=mysql_config['port'],
        target_db=target_db
    )

    #创建Helper 实例
    helper = MySQLHelper(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=target_db,
        port=mysql_config['port']
    )

    # 测试连接
    if not helper.test_connection():
        print(" 连接失败，请检查：")
        print("   1. MySQL 服务是否已启动")
        print("   2. config.json 中的密码是否正确")
        print("   3. 用户名是否正确（通常是 root）")
        return

    #创建测试表
    # 创建 users 表
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(50) NOT NULL,
            age INT,
            email VARCHAR(100)
        )
    """
    helper.execute_non_query(create_table_sql)
    print(" 表 'users' 已创建")

    # 清空表中旧数据，若不清楚则不需要这行
    helper.execute_non_query("DELETE FROM users")
    print(" 已清空旧数据")

    # 插入数据（INSERT）
    data1 = {"name": "张三", "age": 25, "email": "zhangsan@example.com"}
    rows = helper.insert("users", data1)
    print(f"   插入了 {rows} 行数据：{data1['name']}")

    data2 = {"name": "李四", "age": 30, "email": "lisi@example.com"}
    rows = helper.insert("users", data2)
    print(f"   插入了 {rows} 行数据：{data2['name']}")

    data3 = {"name": "王五", "age": 28, "email": "wangwu@example.com"}
    helper.insert("users", data3)
    print(f"   插入了 1 行数据：{data3['name']}")

    # 查询数据（SELECT）
    users = helper.execute_query("SELECT * FROM users")

    print("   当前所有用户：")
    print("   " + "-" * 40)
    for user in users:
        print(f"   ID: {user['id']}, 姓名: {user['name']}, 年龄: {user['age']}, 邮箱: {user['email']}")
    print("   " + "-" * 40)

    # 条件查询
    print("\n   查询年龄大于25的用户：")
    older_users = helper.execute_query("SELECT * FROM users WHERE age > %s", (25,))
    for user in older_users:
        print(f"   → {user['name']}, 年龄: {user['age']}")

    # 更新数据（UPDATE）
    rows = helper.update("users", {"age": 26}, "name = %s", ("张三",))
    print(f"   更新了 {rows} 行数据：张三的年龄改为26")

    # 验证
    zhangsan = helper.execute_query("SELECT * FROM users WHERE name = %s", ("张三",))
    if zhangsan:
        print(f"   张三现在的年龄是 {zhangsan[0]['age']}")

    # 查询单独数据 execute_scalar
    count = helper.execute_scalar("SELECT COUNT(*) FROM users")
    print(f"    用户总数: {count}")

    max_age = helper.execute_scalar("SELECT MAX(age) FROM users")
    print(f"    最大年龄: {max_age}")

    avg_age = helper.execute_scalar("SELECT AVG(age) FROM users")
    print(f"    平均年龄: {avg_age:.1f}")

    # 删除数据（DELETE）
    rows = helper.delete("users", "name = %s", ("李四",))
    print(f"    删除了 {rows} 行数据：李四")

    remaining_users = helper.execute_query("SELECT name FROM users")
    remaining_names = [u['name'] for u in remaining_users]
    print(f"    剩余用户: {', '.join(remaining_names)}")

    # 最终统计
    final_count = helper.execute_scalar("SELECT COUNT(*) FROM users")
    print(f"  最终用户总数: {final_count}")


if __name__ == "__main__":
    demo()