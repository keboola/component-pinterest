from keboola.component import UserException
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

    def __init__(self, token: str = '', refresh_token: str = '', user: str = '', passwd: str = ''):
        if not token:
            if not refresh_token:
                raise UserException('Neither token nor refresh token were available')
            client = HttpClient(base_url=BASE_URL,
                                default_http_header={'Content-Type': 'application/x-www-form-urlencoded'},
                                auth=(user, passwd))
            body = {
                'grant_type': 'refresh_token',
                'refresh_token': refresh_token,
                'scope': 'ads:read'
            }
            response = client.post('oauth/token', data=body)
            if not response.get('access_token'):
                message = response.get('message')
                if not message:
                    message = str(response)
                raise UserException(f'Error retrieving access token from refresh token: {message}')
            token = response.get('access_token')

        AUTH_HEADER['Authorization'] = 'Bearer ' + token
        self.client = HttpClient(base_url=BASE_URL,
                                 default_http_header=DEFAULT_HEADER,
                                 auth_header=AUTH_HEADER)

    def call_client_method(self, method, ep, description='', **kwargs):
        response = self.client._request_raw(method, ep, **kwargs)
        if response:
            return response.json()
        raise UserException(f'HTTP Error {response.status_code} in {description}: ep = {ep}: {response.text}')

    def get_accounts(self):
        request_params = {'page_size': 50}
        total = []
        while True:
            ep = 'ad_accounts'
            response = self.call_client_method('get', ep, params=request_params, description='listing accounts')
            items = response.get('items')
            total.extend(items)
            bookmark = response.get('bookmark')
            if bookmark:
                request_params['bookmark'] = bookmark
            else:
                break
        return total

    def get_templates(self, account_id):
        request_params = {'page_size': 50, 'order': 'DESCENDING'}
        ep = f'ad_accounts/{account_id}/templates'
        total = []
        while True:
            response = self.call_client_method('get', ep, params=request_params, description='listing templates')
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
        response = self.call_client_method('post', ep, json=body, description='creating a report request')
        return response

    def create_request_from_template(self, account_id, template_id, time_range):
        ep = f'ad_accounts/{account_id}/templates/{template_id}/reports'
        response = self.call_client_method('post', ep, json=time_range,
                                           description='creating a report request using a template')
        return response

    def read_report(self, account_id, token):
        """
        response:
            report_status: 'FINISHED' | ...
            url: 'https://pinterest......' odkaz odkud se nacte obsah reportu
        """
        ep = f'ad_accounts/{account_id}/reports'
        request_params = {'token': token}
        response = self.call_client_method('get', ep, params=request_params, description='reading report status')
        return response
