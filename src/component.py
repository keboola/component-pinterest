"""
Template Component main class.

"""
import csv
import datetime
import logging
import os
import time

import dateparser
import requests
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement
from keboola.utils.header_normalizer import DefaultHeaderNormalizer

from Pinterest.client import PinterestClient
from configuration import Configuration, retrieve_keys


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()
        self.cfg: Configuration
        self._pinterest_client: PinterestClient = None

    def run(self):
        """
        Main execution code

        1. Initialize configuration based on parameters provided
        2. Check validity of configuration
        3. Create report requests
            - Use either explicit report specification
            - Or use report templates
        4. Wait until all reports completed
        5. Combine individual reports into resulting CSV table
        6. Write table manifest
        """
        self.__init_configuration()

        # Validate configuration
        if not self.cfg.accounts:
            raise UserException('No accounts for reporting specified')
        if self.cfg.input_variant == "report_specification" and not self.cfg.report_specification.columns:
            raise UserException('No columns selected in report specification')
        if self.cfg.input_variant == "existing_report_ids" and not self.cfg.existing_report_ids:
            raise UserException('No report IDs specified')

        started_reports = []

        if self.cfg.input_variant == 'report_specification':
            report_body = self._prepare_report_body()
            for account_id in self.cfg.accounts:
                logging.info(f"Creating custom report {self.cfg.destination.table_name} in account {account_id}.")
                response = self.client.create_report(account_id=account_id, body=report_body,
                                                     table_name=self.cfg.destination.table_name)
                started_reports.append(dict(key=account_id, account_id=account_id, token=response['token']))
        else:
            time_range_body = self._prepare_time_range_body()
            for item in self.cfg.existing_report_ids:
                account_id, template_id = item.split(':')
                logging.info(f"Creating report from template {template_id} in account {account_id}.")

                response = self.client.create_report_from_template(account_id=account_id,
                                                                   template_id=template_id,
                                                                   time_range=time_range_body)
                started_reports.append(dict(key=template_id, account_id=account_id, token=response['token']))

        reports_to_check = started_reports

        while reports_to_check:
            next_reports = []
            for report in reports_to_check:
                response = self.client.get_report_status(report['account_id'], report['token'])
                status = response['report_status']
                if status == 'IN_PROGRESS':
                    next_reports.append(report)
                    continue
                if status == 'FINISHED':
                    report_url = response['url']
                    raw_output_file = self._local_file(report['key'])
                    self._download_file(report_url, raw_output_file)
            reports_to_check = next_reports
            if reports_to_check:
                time.sleep(10)

        keys, columns = self.check_output_files(started_reports)
        keys.insert(0, 'Account_ID')
        columns.insert(0, 'Account_ID')

        normalizer = DefaultHeaderNormalizer()
        columns = normalizer.normalize_header(columns)
        keys = normalizer.normalize_header(keys)

        table = self.create_out_table_definition(self.cfg.destination.table_name,
                                                 incremental=self.cfg.destination.incremental_loading,
                                                 primary_key=keys,
                                                 columns=columns)

        out_table_path = table.full_path
        os.makedirs(out_table_path, exist_ok=True)
        logging.info("Extraction finished")

        self.combine_output_files(out_table_path, started_reports)

        self.write_manifest(table)

    def __init_configuration(self):
        self.cfg = Configuration.fromDict(parameters=self.configuration.parameters)

    @property
    def client(self):
        """Pinterest client object backing the client property is initialized when it is first accessed.
        Authorization will use '#api_token' parameter if provided.
        Else oauth credentials will be used.
        """
        if not self.configuration.oauth_credentials:
            raise UserException(
                "The authorization is not set up. Please authorize the configuration with your Pinterest account first")
        if not self._pinterest_client:
            api_token = self.configuration.parameters.get('#api_token')
            refresh_token = user = passwd = ''
            if hasattr(self.configuration, "oauth_credentials"):
                user = self.configuration.oauth_credentials.appKey
                passwd = self.configuration.oauth_credentials.appSecret
                refresh_token = self.configuration.oauth_credentials.data.get('refresh_token')
                pass
            self._pinterest_client = PinterestClient(token=api_token,
                                                     refresh_token=refresh_token,
                                                     user=user,
                                                     passwd=passwd)
        return self._pinterest_client

    def _prepare_dates_from_to(self) -> tuple:
        date_from = dateparser.parse(self.cfg.time_range.date_from)
        date_to = dateparser.parse(self.cfg.time_range.date_to)
        return date_from.strftime("%Y-%m-%d"), date_to.strftime("%Y-%m-%d")

    def _prepare_report_body(self):
        start_date, end_date = self._prepare_dates_from_to()
        body = {'start_date': start_date, 'end_date': end_date, 'granularity': self.cfg.time_range.granularity.value,
                'click_window_days': int(self.cfg.report_specification.click_window_days),
                'engagement_window_days': int(self.cfg.report_specification.engagement_window_days),
                'view_window_days': int(self.cfg.report_specification.view_window_days),
                'conversion_report_time': self.cfg.report_specification.conversion_report_time.value,
                'columns': self.cfg.report_specification.columns,
                'level': self.cfg.report_specification.level.value,
                'report_format': 'CSV'}
        return body

    def _prepare_time_range_body(self):
        start_date, end_date = self._prepare_dates_from_to()
        body = {'start_date': start_date, 'end_date': end_date, 'granularity': self.cfg.time_range.granularity.value}
        return body

    @staticmethod
    def _download_file(url: str, result_file_path: str):
        res = requests.get(url, stream=True, timeout=180)
        res.raise_for_status()
        with open(result_file_path, 'wb') as out:
            for chunk in res.iter_content(chunk_size=8192):
                out.write(chunk)

    def _local_file(self, key: str) -> str:
        path = f'{self.files_out_path}/{key}.raw.csv'
        return path

    @staticmethod
    def _destination_file(out_directory: str, key: str) -> str:
        path = f'{out_directory}/{key}.csv'
        return path

    def check_output_files(self, file_descriptors: list) -> tuple:
        """Check consistency of downloaded reports

        Method steps through downloaded reports and check that all have the same header.
        If there is a mismatch it is reported in exception.
        When all went okay, selected header columns are identified as primary key set.

        Args:
            file_descriptors: list of structures (its 'key' attribute has full path to the file) describing
                downloaded reports.

        Returns:
            touple: List of keys, list of columns

        Raises:
            UserException: When there was a mismatch in headers
        """
        header = None
        for item in file_descriptors:
            file = self._local_file(item['key'])
            with open(file, mode='rt') as out_file:
                reader = csv.DictReader(out_file)
                fields = reader.fieldnames
                if not header:
                    header = fields
                else:
                    if fields != header:
                        mm = ''
                        for a, b in zip(fields, header):
                            if a != b:
                                mm = f'{a}/{b}'
                                break
                        raise UserException(f'Headers of reports do not match: {mm}')
        keys = retrieve_keys(header)
        return keys, header

    def combine_output_files(self, out_directory, file_descriptors: list):
        for item in file_descriptors:
            key = item['key']
            account_id = item['account_id']
            file = self._local_file(key=key)
            dest_path = self._destination_file(out_directory=out_directory, key=key)
            with open(file, mode='rt') as in_file, open(dest_path, mode='wt') as out_file:
                reader = csv.reader(in_file)
                next(reader)  # skip header line
                writer = csv.writer(out_file)
                for row in reader:
                    row.insert(0, account_id)
                    writer.writerow(row)
            pass

    @sync_action('load_accounts')
    def load_accounts(self):
        accounts = self.client.list_accounts()
        result = [SelectElement(value=acc['id'], label=f"{acc['name']} ({acc['id']})") for acc in accounts]
        return result

    @sync_action('list_templates')
    def list_templates(self):
        all_templates = []
        for account_id in self.configuration.parameters.get('accounts'):
            account_templates = self.client.list_templates(account_id=account_id)
            all_templates.extend(account_templates)

        result = [SelectElement(
            value=f'{templ["ad_account_id"]}:{templ["id"]}',
            label=f'{templ["name"]} ({templ["ad_account_id"]}:{templ["id"]})')
            for templ in all_templates
        ]
        return result

    @sync_action('list_columns')
    def list_columns(self):
        """List all available columns

        Pinterest API does not provide list of available columns. We use a trick to retrieve it:

        - Create a report request with invalid column list
        - Catch reported exception and retrieve a list of 'good' columns provided there.

        Returns:
             List of all available columns
        """
        account_id = None
        accounts = self.configuration.parameters.get('accounts')
        if accounts:
            account_id = accounts[0]
        if not account_id:
            accounts = self.client.list_accounts()
            if accounts:
                account_id = accounts[0]['id']
        if not account_id:
            raise UserException('It was not possible to find usable account_id')

        start_date = (datetime.date.today() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
        end_date = (datetime.date.today() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
        fake_body = {'start_date': start_date, 'end_date': end_date, 'granularity': 'DAY',
                     'click_window_days': 7,
                     'engagement_window_days': 7,
                     'view_window_days': 7,
                     'conversion_report_time': 'TIME_OF_AD_ACTION',
                     'columns': ['NONSENSE_XXXXXX'],
                     'level': 'CAMPAIGN',
                     'report_format': 'CSV'}

        try:
            self.client.create_report(account_id=account_id, body=fake_body)
        except UserException as ex:
            key = "'NONSENSE_XXXXXX' is not one of ['"
            s = str(ex)
            start = s.find(key)
            if start < 0:
                raise ex
            s = s[start + len(key):]
            end = s.find("']")
            s = s[:end]
            result = [SelectElement(
                value=item,
                label=item)
                for item in s.split("', '")
            ]
            return result

        raise UserException('Failed to generate list of columns')


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
