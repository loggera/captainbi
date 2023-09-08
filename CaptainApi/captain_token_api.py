import help.request_client as hrc
import loguru
import help.redis_helper as hrh

lg = loguru.logger


def get_access_token():
    url = 'https://openapi.captainbi.com/oauth2/token'
    post_data = {
        "client_id": "g2DRFbG5XIWfayPV3M00GoF9pzoMU4Vc",
        "client_secret": "0v9z0aeGqH1jcSIdxCeV0GqHml74qvdj",
        "grant_type": "client_credentials",
        "scope": "all"
    }
    result = hrc.post_amazon_api(url, post_data, {})
    lg.info(f'{result}')
    code = result.get('code')
    if code == 200:
        data_dic = result.get('result')
        access_token = data_dic.get('access_token')
        cache_key = f'Captain:API:Access:Token'
        hrh.next_set_value(cache_key, access_token, 60 * 60 * 1)
        return access_token


if __name__ == '__main__':
    get_access_token()
