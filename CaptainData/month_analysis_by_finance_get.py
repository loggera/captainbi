import CaptainApi.captain_api as captain_api
import help.redis_helper as hrh
import CaptainApi.captain_token_api as captain_token
import loguru

lg = loguru.logger


def get_month_analysis_by_finance(openchannel_id, report_date):
    cache_key = f'Captain:API:Access:Token'
    access_token = hrh.next_get_value(cache_key)
    if not access_token:
        access_token = captain_token.get_access_token()
    cap_api = captain_api.CaptainApi()
    result_list = cap_api.get_list_analysis_by_finance(access_token, openchannel_id, report_date)
    # lg.info(f'{result_list}')
    return result_list
