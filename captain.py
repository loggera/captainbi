import CaptainData.analysis_by_finance_get as cap_finance_data_day
import help.world_hepler as helper_world_db
import help.date_time as hdt
import loguru
import time
import datetime

lg = loguru.logger


def set_cap_finance_data_to_mysql(date):
    # 获取全部open_channel_id
    cap_shop_list = helper_world_db.get_captainbi_shop_msg()
    count = 0
    for cap_shop in cap_shop_list:
        count += 1
        lg.info(f'Count:{count}')
        time.sleep(10)
        open_channel_id = cap_shop.get('open_channel_id')
        lg.info(f'{open_channel_id}')
        result_list = cap_finance_data_day.get_analysis_by_finance(open_channel_id, date)
        lg.info(f'{len(result_list)}')
        if result_list:
            data_list = []
            for i in result_list:
                data = {}
                sku = i.get('sku')
                data['sku'] = sku
                channel_id = i.get('channel_id')
                data['channel_id'] = channel_id
                day_time = str(i.get('time'))
                # day = (lambda x: x.zfill(2) if len(x) != 2 else x)(time_list[1])
                # day_time = f"{time_list[0]}-{day}-01"
                data['time'] = day_time
                sale_sales_quota = i.get('sale_sales_quota')
                data['sale_sales_quota'] = sale_sales_quota
                cpc_cost = i.get('cpc_cost')
                data['cpc_cost'] = cpc_cost
                product_ads_payment_eventlist_charge = i.get('product_ads_payment_eventlist_charge')
                data['product_ads_payment_eventlist_charge'] = product_ads_payment_eventlist_charge
                sale_sales_volume = i.get('sale_sales_volume')
                data['sale_sales_volume'] = sale_sales_volume
                cost_profit_profit = i.get('cost_profit_profit')
                data['cost_profit_profit'] = round(float(cost_profit_profit), 2)
                cost_profit_profit_rate = i.get('cost_profit_profit_rate')
                data['cost_profit_profit_rate'] = cost_profit_profit_rate
                data_list.append(data)
                lg.info(f'{data}')
            helper_world_db.insert_batch_data(data_list, 'captain_asin_day_data')


if __name__ == '__main__':
    start_time = datetime.date(2023, 8, 1)
    end_time = datetime.date(2023, 9, 1)
    while start_time < end_time:
        date = hdt.get_format_data_by_number(start_time)
        # lg.info(f'{date}')
        set_cap_finance_data_to_mysql(date)
        start_time = hdt.add_day(start_time, 1)
