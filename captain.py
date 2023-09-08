import CaptainData.month_analysis_by_finance_get as cap_finance_data
import help.world_hepler as helper_world_db

import loguru

lg = loguru.logger


def set_cap_finance_data_to_mysql():
    # 获取全部open_channel_id
    cap_shop_list = helper_world_db.get_captainbi_shop_msg()
    for cap_shop in cap_shop_list:
        open_channel_id = cap_shop.get('open_channel_id')
        lg.info(f'{open_channel_id}')
        result_list = cap_finance_data.get_month_analysis_by_finance(open_channel_id, 202307)
        lg.info(f'{len(result_list)}')
        if result_list:
            data_list = []
            for i in result_list:
                data = {}
                sku = i.get('sku')
                data['sku'] = sku
                channel_id = i.get('channel_id')
                data['channel_id'] = channel_id
                time_list = str(i.get('time')).split('-')
                day = (lambda x: x.zfill(2) if len(x) != 2 else x)(time_list[1])
                time = f"{time_list[0]}-{day}-01"
                data['time'] = time
                sale_sales_quota = i.get('sale_sales_quota')
                data['sale_sales_quota'] = sale_sales_quota
                cpc_cost = i.get('cpc_cost')
                data['cpc_cost'] = cpc_cost
                product_ads_payment_eventlist_charge = i.get('product_ads_payment_eventlist_charge')
                data['product_ads_payment_eventlist_charge'] = product_ads_payment_eventlist_charge
                sale_sales_volume = i.get('sale_sales_volume')
                data['sale_sales_volume'] = sale_sales_volume
                cost_profit_profit = i.get('cost_profit_profit')
                data['cost_profit_profit'] = cost_profit_profit
                cost_profit_profit_rate = i.get('cost_profit_profit_rate')
                data['cost_profit_profit_rate'] = cost_profit_profit_rate
                data_list.append(data)
            helper_world_db.insert_batch_data(data_list, 'captain_asin_month_data')


if __name__ == '__main__':
    set_cap_finance_data_to_mysql()

