from keboola.http_client import HttpClient

# from pinterest.client import PinterestSDKClient

BASE_URL = 'https://api.pinterest.com/v5'
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

    def get_templates(self, account_id):
        request_params = {'page_size': 2}
        ep = f'ad_accounts/{account_id}/templates'
        total = []
        while True:
            response = self.client.get(ep)
            items = response.get('items')
            total.extend(items)
            bookmark = response.get('bookmark')
            if bookmark:
                request_params['bookmark'] = bookmark
            else:
                break
        return total

    def create_request(self, account_id, body):
        ep = f'ad_accounts/{account_id}/reports'
        response = self.client.post(ep, json=body)
        return response

    def create_request_from_template(self, account_id, template_id, time_range):
        ep = f'ad_accounts/{account_id}/templates/{template_id}/reports'
        response = self.client.post(ep, json=time_range)
        return response

    def read_report(self, account_id, token):
        """
        response:
            report_status: 'FINISHED' | ...
            url: 'https://pinterest......' odkaz odkud se nacte obsah reportu
        """
        ep = f'ad_accounts/{account_id}/reports'
        request_params = {'token': token}
        response = self.client.get(ep, params=request_params)
        return response
