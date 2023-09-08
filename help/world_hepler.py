import help.mysql_helper as mysql
import loguru

lg = loguru.logger
world_data = mysql.DataContext('112.64.174.34', 23306, 'root', 'root@123', 'world')


def get_captainbi_shop_msg():
    sql = f'select * from captainbi_shop_msg'
    data_list = world_data.exec_query_as_dict(sql)
    return data_list


def insert_batch_data(data_list, table_name):
    # 获取字典中的所有键
    keys = data_list[0].keys()
    lg.info(f'{keys}')
    # 构建插入语句的参数部分
    placeholders = ', '.join(['%s'] * len(keys))

    # 构建插入语句
    query = f"INSERT INTO {table_name} ({', '.join(keys)}) VALUES ({placeholders})"
    lg.info(f'{query}')
    # 构建参数列表
    values = [tuple(d.values()) for d in data_list]
    lg.info(f'{values}')
    # 执行批量插入
    world_data.exec_many_non_query(query, values)
