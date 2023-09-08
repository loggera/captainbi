import loguru
import help.mysql_helper as mysql

lg = loguru.logger

# amazon_db = mysql.DataContext('localhost', 3306, 'root', 'root', 'amazon_work')
world_db = mysql.DataContext('112.64.174.34', 23306, 'root', 'root@123', 'world')


def get_campaings_data(shop):
    sql = f'select campaign_id,name,campaign_type,targeting_type,premium_bid_adjustment,daily_budget,state,portfolioId from campaign where shop ="{shop}"'
    date_list = world_db.exec_query_as_dict(sql)
    if date_list is not None and len(date_list) > 0:
        return date_list
    else:
        return None


def get_asin_list_by_asin(asin):
    sql = f'select sku from captainbi_sku where asin ="{asin}" and sku NOT LIKE "amzn.gr%" '
    data_list = world_db.exec_query_as_dict(sql)
    sku_list = []
    if data_list:
        for data in data_list:
            sku = data.get('sku')
            sku_list.append(sku)
    return list(set(sku_list))


def get_asin_info_by_asin(asin):
    sql = f'select sku from captainbi_sku where asin ="{asin}" and sku NOT LIKE "amzn.gr%" and sku not like "%=%"'
    data = world_db.exec_query_one_as_dict(sql)
    if data:
        sku = data.get('sku')
        return sku
    return False


# print(get_asin_info_by_asin('B09TZXZDYX'))

# def create_table(table_name, columns):
#     lg.info(f'{table_name},{columns}')
#     amazon_db.exec_create_table(table_name=table_name, columns=columns)


def set_sp_campaigns_date():
    pass


# def insert_batch_data(data_list, table_name):
#     # 获取字典中的所有键
#     keys = data_list[0].keys()
#     lg.info(f'{keys}')
#     # 构建插入语句的参数部分
#     placeholders = ', '.join(['%s'] * len(keys))
#
#     # 构建插入语句
#     query = f"INSERT INTO {table_name} ({', '.join(keys)}) VALUES ({placeholders})"
#     lg.info(f'{query}')
#     # 构建参数列表
#     values = [tuple(d.values()) for d in data_list]
#     lg.info(f'{values}')
#     # 执行批量插入
#     amazon_db.exec_many_non_query(query, values)


def get_client_id_by_ad_name(ad_name):
    sql = f'select * from ad_api_message  where ad_name ="{ad_name}"'
    data = world_db.exec_query_one_as_dict(sql)
    return data


# print(get_client_id_by_ad_name('TooCust-mx'))

def get_portfolio_id_by_portfolio(portfolio):
    sql = f'select Portfolio_id from portfolios where Portfolio_name="{portfolio}"'
    data = world_db.exec_query_one_as_dict(sql)
    if data:
        portfolio_id = data.get('Portfolio_id')
        return portfolio_id
    return None


# 调价规则写入
def set_change_price_strategy(behavior, rule_id, week, start_date, end_date, start_time, end_time, strategy, bid,
                              state):
    api_rep = {'status': 'unknown', 'message': '未知错误'}
    # 增加规则
    if behavior == 'add':
        if not rule_id:
            # 规则是否存在
            data = get_change_price_strategy(week, start_time, end_time, strategy, bid, start_date, end_date, state)
            if data:
                api_rep['status'] = 'failed'
                api_rep['message'] = '该规则已经存在，请勿重复创建！'
                return api_rep
            # 写入规则
            sql = f'insert into adjust_price_rule (Week,StartDate,EndDate,StartTime,EndTime,Strategy,BId,State) VALUES ("{week}","{start_date}","{end_date}","{start_time}","{end_time}","{strategy}","{bid}","{state}")'
            world_db.exec_non_query(sql)
            api_rep['status'] = 'success'
            api_rep['message'] = '创建成功！'
            return api_rep
    # 规则是否存在
    data = get_change_price_strategy_by_id(rule_id)
    if not data:
        api_rep['status'] = 'failed'
        api_rep['message'] = '该规则已经不存在，请删除重新创建！'
        return api_rep
    # 更新规则
    if behavior == 'update':
        sql = f'update adjust_price_rule set Week = "{week}",StartTime ="{start_time}",EndTime ="{end_time}",Strategy ="{strategy}",BId ="{bid}",StartDate ="{start_date}",EndDate ="{end_date}",State ="{state}" where id = {rule_id}'
        world_db.exec_non_query(sql)
        api_rep['status'] = 'success'
        api_rep['message'] = '创建成功！'
        return api_rep
    # 删除规则
    elif behavior == 'delete':
        sql = f'DELETE FROM adjust_price_rule WHERE id = {rule_id}'
        world_db.exec_non_query(sql)
        api_rep['status'] = 'success'
        api_rep['message'] = '删除成功！'
        return api_rep
    api_rep['status'] = 'failed'
    api_rep['message'] = '未知操作！'
    return api_rep


# 根据条件查询调价规则
def get_change_price_strategy(week, start_time, end_time, strategy, bid, start_date, end_date, state):
    sql = f'Select * from adjust_price_rule where Week ="{week}" and StartDate ="{start_date}" and  EndDate = "{end_date}" and StartTime ="{start_time}" and  EndTime = "{end_time}" and Strategy ="{strategy}" and BId ="{bid}" and State ="{state}"'
    lg.info(f'{sql}')
    data = world_db.exec_query_one_as_dict(sql)
    return data


# 根据rule_id 查询调价规则
def get_change_price_strategy_by_id(rule_id):
    sql = f'Select * from adjust_price_rule where id = {rule_id}'
    data = world_db.exec_query_one_as_dict(sql)
    return data


# 获取全部调价规则列表
def get_adjust_strategy_list():
    sql = f'Select id as RuleId ,Week,DATE_FORMAT(StartDate, "%Y-%m-%d %H:%i:%s") as StartDate,DATE_FORMAT(EndDate, "%Y-%m-%d %H:%i:%s") as EndDate,StartTime,EndTime,Strategy,BId,State from adjust_price_rule'
    data_list = world_db.exec_query_as_dict(sql)
    if data_list:
        for data in data_list:
            start_time = data.get('StartTime')
            end_time = data.get('EndTime')
            data['Time'] = f'{start_time} - {end_time}'
    return data_list


# 待调价的数据写入
def set_adjust_price_data(rule_id, campaign_id, adgroup_id, keyword_id, def_bid, state, keyword_text, shop_name,
                          match_type, status):
    api_rep = {'status': 'unknown', 'message': '未知错误'}
    sql = f'insert into adjust_price_data (RuleId,CampaignId,AdgroupId,KeywordId,DefBid,State,KeywordText,ShopName,MatchType,Status) Values ({rule_id},"{campaign_id}","{adgroup_id}","{keyword_id}","{def_bid}","{state}","{keyword_text}","{shop_name}","{match_type}","{status}")'
    world_db.exec_non_query(sql)
    api_rep['status'] = 'success'
    api_rep['message'] = '创建成功！'
    return api_rep


def get_adjust_data(campaign_id, ad_group_id, keyword_id, state, status):
    sql = f'select * from adjust_price_data where 1=1'
    if campaign_id:
        sql += f' and CampaignId = "{campaign_id}" '
    if ad_group_id:
        sql += f' and AdgroupId = "{ad_group_id}" '
    if keyword_id:
        sql += f' and KeywordId = "{keyword_id}" '
    if state:
        sql += f' and State = "{state}" '
    if status:
        sql += f' and Status = "{status}"'
    data_list = world_db.exec_query_as_dict(sql)
    return data_list


def change_adjust_data(pk_id, status, keyword_id, behavior):
    api_rep = {'status': 'unknown', 'message': '未知错误'}
    search_sql = f'select * from adjust_price_data where id =-1'
    if pk_id:
        search_sql = f'select * from adjust_price_data where id ={pk_id}'
    if keyword_id:
        search_sql = f'select * from adjust_price_data where KeywordId ="{keyword_id}"'
    data = world_db.exec_query_one_as_dict(search_sql)

    if not data:
        api_rep['status'] = 'failed'
        api_rep['message'] = '数据不存在，请检查数据的Id!'
        return api_rep
    bh_sql = None
    if behavior == 'detele':
        if pk_id:
            bh_sql = f'DELETE FROM adjust_price_data where id = {pk_id}'
        if keyword_id:
            bh_sql = f'DELETE FROM adjust_price_data where KeywordId = "{keyword_id}" '
    if behavior == 'update':
        if status and pk_id:
            bh_sql = f'update adjust_price_data set Status ="{status}" where id ={pk_id}'
        if status and keyword_id:
            bh_sql = f'update adjust_price_data set Status ="{status}" where KeywordId ="{keyword_id}"'
    if not bh_sql:
        api_rep['status'] = 'failed'
        api_rep['message'] = '没有对应的操作'
        return api_rep
    world_db.exec_non_query(bh_sql)
    api_rep['status'] = 'success'
    api_rep['message'] = '操作成功'
    return api_rep


def get_strategy_by_week_day(week):
    sql = f'select * from adjust_price_rule where Week = "{week}"'
    data_list = world_db.exec_query_as_dict(sql)
    return data_list


def get_adjust_data_by_rule_id_change(rule_id):
    sql = f'select * from adjust_price_data where RuleId ={rule_id}  and Status ="启用"  and (ChangeStatus IS NUll OR ChangeStatus=0)'
    data_list = world_db.exec_query_as_dict(sql)
    return data_list


def get_adjust_data_by_rule_id_back(rule_id):
    sql = f'select * from adjust_price_data where RuleId ={rule_id}  and Status ="启用"  and(BackStatus IS NULL OR BackStatus = 0)'
    data_list = world_db.exec_query_as_dict(sql)
    return data_list


def update_adjust_deal_status(pk_id, change_status, back_status):
    sql = f'update adjust_price_data set ChangeStatus ={change_status},BackStatus={back_status} where id ={pk_id}'
    world_db.exec_non_query(sql)
