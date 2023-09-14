import help.request_client as hrc
import CaptainApi.captain_token_api as cta
import loguru

lg = loguru.logger


class CaptainApi:
    def __init__(self):
        self.base_url = 'https://openapi.captainbi.com'

    def get_month_analysis_by_finance(self, access_token, openchannel_id, page, rows, report_date):
        url = f'{self.base_url}/v1/open_goods_finance/get_month_analysis_by_finance'
        post_data = {
            "page": page,
            "rows": rows,
            "report_date": report_date
        }
        headers = {"Authorization": f"Bearer {access_token}",
                   "OpenChannelId": f"{openchannel_id}"}
        result = hrc.get_api_have_params(url, headers, post_data)
        return result

    def get_list_month_analysis_by_finance(self, access_token, openchannel_id, report_date):
        page = 1
        rows = 100
        status = True
        finance_list = []
        while status:
            result = self.get_month_analysis_by_finance(access_token, openchannel_id, page, rows, report_date)
            if result:
                data = result.get('data')
                if data and len(data) != 0:
                    finance_list.extend(data)
                    page += 1
                else:
                    status = False
            else:
                status = False
        return finance_list

    def get_analysis_by_finance(self, access_token, openchannel_id, page, rows, report_date):
        url = f'{self.base_url}/v1/open_goods_finance/get_analysis_by_finance'
        post_data = {
            "page": page,
            "rows": rows,
            "report_date": report_date
        }
        headers = {"Authorization": f"Bearer {access_token}",
                   "OpenChannelId": f"{openchannel_id}"}
        result = hrc.get_api_have_params(url, headers, post_data)
        return result

    def get_list_analysis_by_finance(self, access_token, openchannel_id, report_date):
        page = 1
        rows = 100
        status = True
        finance_list = []
        while status:
            result = self.get_analysis_by_finance(access_token, openchannel_id, page, rows, report_date)
            if result:
                data = result.get('data')
                if data and len(data) != 0:
                    finance_list.extend(data)
                    page += 1
                else:
                    status = False
            else:
                status = False
        return finance_list
