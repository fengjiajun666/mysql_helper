"""
MySQL Helper 工具类
"""


# 导入需要的库
import pymysql

# 定义 MySQLHelper 类
class MySQLHelper:
    def __init__(self, host, user, password, database, port=3306, charset='utf8mb4'):
        self.host = host           # 保存主机地址
        self.user = user           # 保存用户名
        self.password = password   # 保存密码
        self.database = database   # 保存数据库名
        self.port = port           # 保存端口号
        self.charset = charset     # 保存字符集
        self.conn = None   # conn 用来存放数据库连接对象
        self.cursor = None # cursor 用来存放游标对象

    def _connect(self):
        try:
            self.conn = pymysql.connect(
                host=self.host,           # 服务器地址
                user=self.user,           # 用户名
                password=self.password,   # 密码
                database=self.database,   # 要连接的数据库（可以为空）
                port=self.port,           # 端口号
                charset=self.charset,     # 字符集
                cursorclass=pymysql.cursors.DictCursor  # 让返回的结果是字典格式
            )
            # 创建游标（cursor）
            self.cursor = self.conn.cursor()

        except Exception as e:
            # 如果连接失败
            print(f"数据库连接失败: {e}")
            raise e

    # close
    def _close(self):
        # 每次执行完 SQL 操作后，都应该关闭连接，释放资源
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    # 测试连接方法
    def test_connection(self):
        try:
            self._connect()
            print(" 数据库连接成功")
            self._close()
            # 返回 True 表示成功
            return True
        except Exception as e:
            print(f"连接失败：{e}")
            # 返回 False 表示失败
            return False

    # 执行非查询 SQL（INSERT、UPDATE、DELETE）
    def execute_non_query(self, sql, params=None):
        """
        :param sql:    要执行的 SQL 语句，可以使用 %s 作为占位符
        :param params: 可选参数，用于替换 SQL 中的占位符，可以是元组或列表
        :return: 受影响的行数（比如 INSERT 成功返回 1，UPDATE 修改了 3 行就返回 3）
        """
        try:
            self._connect()
            if params:
                rows = self.cursor.execute(sql, params)
            else:
                rows = self.cursor.execute(sql)
            #  INSERT、UPDATE、DELETE，必须调用 commit() 才能让更改真正生效
            self.conn.commit()
            return rows

        except Exception as e:
            # 如果执行过程中出错，需要回滚（rollback）
            self.conn.rollback()
            print(f"执行失败：{e}")
            raise e

        finally:
            # 关闭连接，释放资源
            self._close()

    # 执行查询 SQL（SELECT）
    def execute_query(self, sql, params=None):
        """
        参数说明：
        :param sql:    SELECT 查询语句，可以使用 %s 作为占位符
        :param params: 可选参数，用于替换占位符
        :return: 查询结果列表，列表中的每一项是一个字典（键是列名，值是对应的值）
        """
        try:
            self._connect()
            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
            # fetchall() 用于获取所有查询结果，返回字典列表
            result = self.cursor.fetchall()

            return result

        except Exception as e:
            print(f"查询失败：{e}")
            raise e

        finally:
            self._close()

    # 执行查询，返回单个值（Scalar）
    def execute_scalar(self, sql, params=None):
        """
        执行查询，返回结果集中的第一行第一列的值   SELECT COUNT(*)、SELECT MAX(id)
        参数说明：
        :param sql:    查询语句
        :param params: 可选参数
        :return: 第一行第一列的值，如果没有数据则返回 None
        """
        try:
            self._connect()

            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)

            # fetchone() 获取第一行数据
            row = self.cursor.fetchone()
            # 如果存在数据，则返回第一列的值
            # row 是字典，row.values() 是所有值的列表   list(row.values()) 把值转换成列表，[0] 取第一个值
            if row:
                return list(row.values())[0]
            else:
                return None

        except Exception as e:
            print(f"查询失败：{e}")
            raise e

        finally:
            self._close()

    # 插入
    def insert(self, table, data):
        """
        :param table: 表名（字符串）
        :param data:  要插入的数据，字典格式，键是列名，值是要插入的值
        :return: 受影响的行数
        """
        # data.keys() 获取所有列名，', '.join(data.keys()) 把列名用逗号连接
        columns = ', '.join(data.keys())

        # 创建占位符，每个列对应一个 %s
        # ['%s', '%s', '%s'] 然后 join 变成 "%s, %s, %s"
        placeholders = ', '.join(['%s'] * len(data))

        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

        # data.values() 获取所有要插入的值，转换成元组
        return self.execute_non_query(sql, tuple(data.values()))

    # 更新
    def update(self, table, data, where_clause, params=None):
        """
        :param table:         表名
        :param data:          要更新的数据，字典格式
        :param where_clause:  WHERE 条件（字符串），可以使用 %s 占位符
        :param params:        WHERE 条件中的参数（元组形式）
        :return: 受影响的行数
        """
        # 构建 SET 子句：将每个 "列名=%s" 用逗号连接
        set_clause = ', '.join([f"{key}=%s" for key in data.keys()])

        sql = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"

        # 参数合并：先传 data 的值，再传 where 条件的参数
        all_params = tuple(data.values()) + (params if params else ())

        return self.execute_non_query(sql, all_params)

    # 删除
    def delete(self, table, where_clause, params=None):

        sql = f"DELETE FROM {table} WHERE {where_clause}"
        return self.execute_non_query(sql, params)


#测试代码
if __name__ == "__main__":
    print("请运行 demo.py 来查看使用示例")