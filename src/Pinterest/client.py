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

    def create_request(self, account_id, body):
        ep = f'ad_accounts/{account_id}/reports'
        response = self.client.post(ep, json=body)
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


if __name__ == '__main__':
    TOKEN = 'pina_AEA5FJQWAATGQAQAGDAJODLWPKHK5CABACGSOWK2SH6L5HANXBIT7FOUTVETMSUT7KUY7SA36HAQGWG4NRQZYC3I7QBXRPYA'

    body = {
        "start_date": "2020-12-20",
        "end_date": "2020-12-20",
        "granularity": "TOTAL",
        "columns": [
            "CAMPAIGN_NAME", "SPEND_IN_MICRO_DOLLAR"
        ],
        "level": "CAMPAIGN",
        "report_format": "CSV"
    }

    client = PinterestClient(TOKEN)
    # response = client.create_request('549765938918', body)

    report_token = "aicYIR-uD7NJ_rphdEHgGA==PFT4JVYEPSigoMZ54AMtCJ_twkY6uPCI8nS3A5dSG3ZnW5lhR04oEOUbJl2Us4si3pnWNQmkmoEkNy_JciMmWVP97dnxmjznfHYnpOIEmSuS-7Ny_Gsg10KeluI_bEyRlCtYczuzjNTjQO-j7_NhMiJ2Ym1kNrDpzG80PYAax7d6U7A5r0aRw_G28bqnFeGeXe4cGLWMf2z8jZKIbHcF1oOA8JwA3tGvtPBt2FSGIQzVV4ZSvaK0diSeUV0qoow4vq3lM3sw98q3a2nz9aT0GsIn4kEFmRSA1q4mikxoxe6eBBeYM5iCSeK3Jansa4HmK05SaPRvfKv4Bs2YOzQ0vuoCOqvH-pI7h-ohar6-BKbJlcvIcNgClIte03JGq-L-ZG4KgA03b3ZTY-08j29lRaJv_hjUT47BVvZ7RctoKTQ="
    response = client.read_report('549765938918', report_token)

    pass
