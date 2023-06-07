from keboola.http_client import HttpClient
# from pinterest.client import PinterestSDKClient

BASE_URL = 'https://api-sandbox.pinterest.com/v5'
DEFAULT_HEADER = {
    'Content-Type': 'application/json'
}
AUTH_HEADER = {
    'Authorization': None
}


class PinterestClient:
    """
    Instance of this class provieds a service object that is responsible
    for all commulnication to pinterest API.
    """

    def __init__(self, token: str):
        AUTH_HEADER['Authorization'] = 'Bearer ' + token
        self.client = HttpClient(base_url=BASE_URL,
                                 default_http_header=DEFAULT_HEADER,
                                 auth_header=AUTH_HEADER)

    def get_accounts(self):
        request_params = {'page_size': 2}
        total = []
        while True:
            response = self.client.get('ad_accounts', params=request_params)
            items = response.get('items')
            total.extend(items)
            bookmark = response.get('bookmark')
            if bookmark:
                request_params['bookmark'] = bookmark
            else:
                break
        return total


if __name__ == '__main__':
    TOKEN = 'pina_AMA5FJQWAAASMAQAGCANADHFSSYZBCABACGSP34KFPFOV2B7L54DQBZKIGX5CHJT6JRVFEQDFLQHQLKCZLK2JEPVSEFMQMIA'

    client = PinterestClient(TOKEN)
    response = client.get_accounts()

    pass
