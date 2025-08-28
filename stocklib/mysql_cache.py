import pandas as pd
import pymysql
from sqlalchemy import create_engine
import os
import pickle
from typing import Optional
import traceback
from sqlalchemy import text  # 新增导入


class MySQLCache:
    """MySQL缓存类，用于将DataFrame数据缓存到文件和MySQL数据库"""

    def __init__(
            self,
            db_user: str = 'root',
            db_password: str = 'my_password',
            db_host: str = '127.0.0.1',
            db_port: int = '3306',
            db_name: str = 'stock_info',
            cache_dir: str = './cache',
            market: str = 'SH'
    ):
        """
        初始化MySQL缓存对象

        Args:
            db_user: MySQL用户名
            db_password: MySQL密码
            db_host: MySQL主机地址
            db_port: MySQL端口
            db_name: 数据库名称
            cache_dir: 缓存文件存储目录
        """
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self.market = market
        self.cache_switch = True

        self.engine = create_engine(
            f'mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}',
            pool_size=20,  # 连接池大小
            max_overflow=10,  # 最大溢出连接数
            pool_recycle=3600,  # 连接回收时间（秒）
            pool_timeout=30,  # 获取连接超时时间
            pool_pre_ping=True  # 使用前验证连接有效性
        )

    def write_to_cache(
            self,
            date: str,
            report_type: str,
            data: pd.DataFrame,
            force: bool = False,
            market: str = 'SH',
            file_type: str = 'pkl'
    ):
        """
        将DataFrame数据同时写入文件缓存和MySQL数据库

        Args:
            date: 数据日期，用于分区和标识
            report_type: 报告类型，用于构建缓存路径
            data: 要缓存的DataFrame数据
            force: 是否强制覆盖已存在的数据
            file_type: 文件缓存类型，默认为pkl
        """
        if self.cache_switch == False:
            return
        table_name =  self._get_table_name(date, report_type, file_type,market)


        if table_name.startswith("history"):
            if 'ma_signal' in data.columns:
                data = data.rename(columns={'ma_signal': 'mac_signal'})
            if 'mac_signal' in data.columns and 'MAC_Signal' in data.columns:
                data = data.rename(columns={'MAC_Signal': 'SMA_signal'})
            return

        try:
            data = self._convert_to_df( data)
            # MySQL写入逻辑
            if not self._table_exists(table_name):
                self._create_table_from_df(table_name, data)

            # if not force and self._table_has_data(table_name, date):

            self._insert_data_to_table(table_name, data, date)
            print(f"[MYSQL] 已将 {report_type} 数据写入表: {table_name}")
        except Exception as e:
            print(f"[MYSQL] 写入数据库失败: {table_name}, 错误: {e}")
            traceback.print_exc()

    def read_from_cache(
            self,
            date: str,
            report_type: str,
            market: str =  'SH',
            file_type: str = 'pkl',
            conditions = None
    ) -> Optional[pd.DataFrame]:
        """
        从缓存读取数据，优先从MySQL读取

        Args:
            date: 数据日期
            report_type: 报告类型
            file_type: 文件类型，用于构建缓存路径

        Returns:
            缓存的DataFrame数据，如果不存在则返回None
        """
        if self.cache_switch == False:
            return pd.DataFrame()

        table_name = self._get_table_name(date, report_type, file_type,market)
        code = ''
        if table_name.startswith("history"):
            # 修改表名为history_self.market
            parts = table_name.split('_')
            code = parts[1]
            return pd.DataFrame()
        try:
            # 优先从MySQL读取
            if self._table_exists(table_name) and self._table_has_data(table_name, date):
                if table_name.startswith("history"):
                    return self._read_history_from_mysql(table_name, date,code)
                return self._read_from_mysql(table_name, date,conditions = conditions)
        except Exception as e:
            print(f"[MYSQL] 从数据库读取失败: {table_name}, 错误: {e}")
        return None

    def _get_table_name(self, date: str, report_type: str, file_type: str,market) -> str:
        """构建缓存文件路径"""
        if report_type.startswith("history"):
            # 修改表名为history_self.market
            table_name = f"history_{market}"
        elif report_type.startswith("zcfz") or report_type.startswith("lrb") or report_type.startswith("xjll"):
            table_name = f"{report_type}_{market}"
        elif report_type.startswith("financial"):
            table_name = f"financial_{market}"
        elif "spot_em_zh_df" in report_type:
            table_name = f"spot_em_zh_df_{market}"
        elif report_type.startswith("stock_concept")  or report_type.startswith("stock_industry") :
            table_name = f"{report_type}_{market}"
        elif report_type.startswith("stock_famous_"):
            table_name = f"stock_famous__{market}"
        else:
            table_name = f"{report_type}_{date}_{market}"
        return table_name

    def _get_mysql_engine(self):
        """获取MySQL数据库连接引擎"""
        return  self.engine;

    def _table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        with self._get_mysql_engine().connect() as conn:
            try:
                sql = "SELECT COUNT(*) FROM information_schema.tables "+f"WHERE table_schema = '{self.db_name}' AND table_name = '{table_name}'"

                result = conn.execute(
                   text(sql)
                ).scalar()
                return result > 0
            except Exception as e:
                print(f"[MYSQL] 检查表是否存在失败: {table_name}, 错误: {e}")
                traceback.print_exc()
                return False

    def _create_table_from_df(self, table_name: str, df: pd.DataFrame):
        """根据DataFrame结构创建MySQL表"""
        dtype_mapping = {
            'int64': 'BIGINT',
            'float64': 'DOUBLE',
            'datetime64[ns]': 'DATETIME',
            'bool': 'TINYINT(1)'
        }

        columns = []
        for col, dtype in df.dtypes.items():
            col_dtype = str(dtype)

            if col_dtype == 'object':
                # 计算列的最大长度
                max_len = df[col].dropna().apply(lambda x: len(str(x))).max() if not df[col].dropna().empty else 0

                # 根据最大长度选择合适的类型
                if max_len < 128:
                    sql_type = 'VARCHAR(128)'
                elif 128 <= max_len <= 1024:
                    sql_type = 'VARCHAR(1024)'
                else:
                    sql_type = 'TEXT'
            else:
                sql_type = dtype_mapping.get(col_dtype, 'TEXT')

            columns.append(f'`{col}` {sql_type}')

        # 添加自增ID列和日期分区列
        columns.append('`id` BIGINT AUTO_INCREMENT PRIMARY KEY')
        columns.append('`date_partition` DATE NOT NULL')

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            {', '.join(columns)}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """

        with self._get_mysql_engine().connect() as conn:
            conn.execute(text(create_table_sql))

    def _table_has_data(self, table_name: str, date: str, conditions: dict = None) -> bool:
        """检查特定日期是否已有数据（支持任意字段的自定义条件）"""
        query = f"SELECT COUNT(*) FROM `{table_name}` WHERE `date_partition` = :date"
        params = {"date": date}

        if conditions:
            for field, value in conditions.items():
                query += f" AND `{field}` = :{field}"  # 动态添加字段条件
                params[field] = value

        with self._get_mysql_engine().connect() as conn:
            result = conn.execute(text(query), params).scalar()
            return result > 0
    def _insert_data_to_table(self, table_name: str, df: pd.DataFrame, date: str):
        """将DataFrame数据插入到MySQL表"""
        # 添加日期分区列
        df = df.copy()
        df['date_partition'] = date

        # 使用SQLAlchemy插入数据
        try:
            df.to_sql(
                name=table_name,
                con=self._get_mysql_engine(),
                if_exists='append',
                index=False,
                chunksize=1000  # 每批插入1000条记录
            )
        except Exception as e:
            print(f"[MYSQL] 写入数据库失败: {table_name}, 错误: {e}")
            traceback.print_exc()
    def _read_from_mysql(self, table_name: str, date: str,conditions = None) -> pd.DataFrame:
        """从MySQL读取数据"""
        engine = self._get_mysql_engine()
        sql = f"SELECT * FROM `{table_name}` WHERE `date_partition` = :date"
        params = {"date": date}

        params = {"date": date}
        if conditions:
            for field, value in conditions.items():
                sql += f" AND `{field}` = :{field}"  # 动态添加字段条件
                params[field] = value

        try:
            query = text(sql)
            df = pd.read_sql(query, engine, params=params)
            return df
        except Exception as e:
            print(f"[MYSQL] 读取数据库失败: {table_name}, 错误: {e}")
            return pd.DataFrame()

    def _read_history_from_mysql(self, table_name: str, date: str,code:str) -> pd.DataFrame:
        """从MySQL读取数据"""
        engine = self._get_mysql_engine()
        query = text(f"SELECT * FROM `{table_name}` WHERE `date_partition` = :date" +
                     f" AND `股票代码` = :code")
        try:
            df = pd.read_sql(query, engine, params={"date": date,"code":code})
            return df
        except Exception as e:
            print(f"[MYSQL] 读取数据库失败: {table_name}, 错误: {e}")
            traceback.print_exc()
            return pd.DataFrame()

    def _convert_to_df(self, data) -> pd.DataFrame:
        """将数据转换为DataFrame"""
        if isinstance(data, list):
            if not data:
                raise ValueError("数据列表为空")
            # 检查列表中的元素是否为DataFrame
            if all(isinstance(df, pd.DataFrame) for df in data):
                df = pd.concat(data, ignore_index=True)
            else:
                # 如果列表元素不是DataFrame，尝试转换
                df = pd.DataFrame(data)
        elif isinstance(data, pd.DataFrame):
            df = data
        else:
            raise TypeError("数据类型不支持，需要DataFrame或DataFrame列表")
        return df
