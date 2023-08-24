from keboola.component import UserException
from keboola.http_client import HttpClient
import re

BASE_URL = 'https://api.pinterest.com/v5'
DEFAULT_HEADER = {
    'Content-Type': 'application/json'
}
AUTH_HEADER = {
    'Authorization': None
}


class PinterestClient:
    """ Instance of this class provides a service object that is responsible
    for all communication to Pinterest API.
    """

    def __init__(self, token: str = '', refresh_token: str = '', user: str = '', passwd: str = ''):
        """Initialize HttpClient authorization based either on token (if supplied) or a refresh token
        (if token was missing).

        Args:
            token: Access token - when supplied it is used in authentication header directly
            refresh_token: Used to retrieve access token when token parameter was not provided
            user: Used with refresh token only
            passwd: Used with refresh token only

        Returns:
            Initialized HttpClient object.

        """
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

    def _call_client_method(self, method: str, ep: str, description: str = '', table_name: str = '', **kwargs):
        """This is a wrapper around request method provided by the HttpClient.
        Its purpose is to filter our specifically problem of incompatible selected columns.
        If method fails it is converted to UserException exception with more elaborated error message.

        Args:
            method: Either 'GET' or 'POST'
            ep: API endpoint
            description: Verbal description of a context when method is called
            table_name: Destination table name - used to identify source of error in the log

        Returns:
            API response on success

        Raises:
            UserException: In case of error response

        """
        response = self.client._request_raw(method, ep, **kwargs)
        if response:
            return response.json()
        msg_columns = re.search('Columns .* are not available.', response.text)
        if msg_columns:
            message = f'Failed to create report {table_name}: {msg_columns.group()} Some metric & dimension ' \
                      f'combinations aren\'t supported. To create more complex reports it is recommended ' \
                      f'to use Pinterest Custom reports directly in the Pinterest platform.'
        else:
            message = f'HTTP Error {response.status_code} in {description}: ep = {ep}: {response.text}'

        raise UserException(message)

    def list_accounts(self) -> list:
        """List ad accounts

        List all ad accounts that either belong to logged in client or the client was granted access to.

        API documentation: https://developers.pinterest.com/docs/api/v5/#operation/ad_accounts/list

        Returns:
            List of structures with ad accounts details. Each structure contains 'id' and 'name' attributes
            (among other details).

        Raises:
            UserException: If error occurred - either specification problem or communication problem
        """
        request_params = {'page_size': 50}
        total = []
        while True:
            ep = 'ad_accounts'
            response = self._call_client_method('get', ep, params=request_params, description='listing accounts')
            items = response.get('items')
            total.extend(items)
            bookmark = response.get('bookmark')
            if bookmark:
                request_params['bookmark'] = bookmark
            else:
                break
        return total

    def list_templates(self, account_id: str) -> list:
        """List templates

        API documentation: https://developers.pinterest.com/docs/api/v5/#operation/templates/list

        Args:
            account_id: Account ID

        Returns:
            List of all templates the account has access to. Each element has 'id' and 'name' attributes
            (among other details).

        Raises:
            UserException: If error occurred - either specification problem or communication problem
        """
        request_params = {'page_size': 50, 'order': 'DESCENDING'}
        ep = f'ad_accounts/{account_id}/templates'
        total = []
        while True:
            response = self._call_client_method('get', ep, params=request_params, description='listing templates')
            items = response.get('items')
            total.extend(items)
            bookmark = response.get('bookmark')
            if bookmark:
                request_params['bookmark'] = bookmark
            else:
                break
        return total

    def create_report(self, account_id: str, body: dict, table_name='') -> dict:
        """Create async request for an account analytics report

        API documentation: https://developers.pinterest.com/docs/api/v5/#operation/analytics/create_report

        This API creates a request for specific report.

        Args:
            account_id: Account ID to be used
            body: Requested report parameters
            table_name: Name of resulting Keboola table - identification when exception was raised

        Returns:
            Structure with 'token' attribute. The token is used for querying report status

        Raises:
            UserException: If error occurred - either report specification problem or communication problem
        """
        ep = f'ad_accounts/{account_id}/reports'
        response = self._call_client_method('post', ep, json=body, description='creating a report request',
                                            table_name=table_name)
        return response

    def create_report_from_template(self, account_id: str, template_id: str, time_range):
        """Create async request for an analytics report using a template

        API documentation: https://developers.pinterest.com/docs/api/v5/#operation/analytics/create_template_report

        This API creates a request for a report template (created in web app)

        Args:
            account_id: Account ID to be used
            template_id: Template identification
            time_range: Structure having 'start_date', 'end_date' and 'granularity' attributes

        Returns:
            Structure with 'token' attribute. The token is used for querying report status

        Raises:
            UserException: If error occurred - either specification problem or communication problem
        """
        ep = f'ad_accounts/{account_id}/templates/{template_id}/reports'
        response = self._call_client_method('post', ep, json=time_range,
                                            description='creating a report request using a template')
        return response

    def get_report_status(self, account_id: str, token: str):
        """Get the account analytics report created by the async call

        API documentation: https://developers.pinterest.com/docs/api/v5/#operation/analytics/create_template_report

        This API returns status of a report. Report may be in one of states:
        DOES_NOT_EXIST, FINISHED, IN_PROGRESS, EXPIRED, FAILED, CANCELLED

        For 'FINISHED' state it also returns URL from where to download the report data.

        Args:
            account_id: Account ID to be used
            token: Token identifying async report

        Returns:
            Structure with 'report_status' and 'url' attributes. The 'The token is used for querying report status

        Raises:
            UserException: If error occurred - either specification problem or communication problem
        """
        ep = f'ad_accounts/{account_id}/reports'
        request_params = {'token': token}
        response = self._call_client_method('get', ep, params=request_params, description='reading report status')
        return response
