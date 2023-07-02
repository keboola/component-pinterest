"""
Template Component main class.

"""
import os
import csv
import logging
import time
from datetime import datetime

import requests
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement
import dateparser

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
        self.cfg: Configuration = None
        self._pinterest_client: PinterestClient = None

    @property
    def client(self):
        if not self._pinterest_client:
            api_token = self.configuration.parameters.get('#api_token')
            self._pinterest_client = PinterestClient(api_token)
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
        # body['click_window_days'] = int(body['click_window_days'])
        # body['engagement_window_days'] = int(body['engagement_window_days'])
        # body['view_window_days'] = int(body['view_window_days'])
        # body['report_format'] = 'CSV'
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

    def run(self):
        """
        Main execution code
        """

        params = self.configuration.parameters
        self.cfg = Configuration.fromDict(parameters=params)

        # get last state data/in/state.json from previous run
        # previous_state = self.get_state_file()
        # logging.info(previous_state.get('some_state_parameter'))

        started_reports = []

        if self.cfg.input_variant == 'report_specification':
            report_body = self._prepare_report_body()
            for account_id in self.cfg.accounts:
                response = self.client.create_request(account_id=account_id, body=report_body)
                started_reports.append(dict(key=account_id, account_id=account_id, token=response['token']))
                pass
        else:
            time_range_body = self._prepare_time_range_body()
            for item in self.cfg.existing_report_ids:
                account_id, template_id = item.split(':')
                response = self.client.create_request_from_template(account_id=account_id,
                                                                    template_id=template_id,
                                                                    time_range=time_range_body)
                started_reports.append(dict(key=template_id, account_id=account_id, token=response['token']))
            pass

        reports_to_check = started_reports

        while reports_to_check:
            next_reports = []
            for report in reports_to_check:
                response = self.client.read_report(report['account_id'], report['token'])
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
        keys.insert(0, 'Account ID')
        columns.insert(0, 'Account ID')

        table = self.create_out_table_definition(self.cfg.destination.table_name,
                                                 incremental=self.cfg.destination.incremental_loading,
                                                 primary_key=keys,
                                                 columns=columns)

        out_table_path = table.full_path
        os.makedirs(out_table_path, exist_ok=True)
        logging.info(out_table_path)

        self.combine_output_files(out_table_path, started_reports)

        # DO whatever and save into out_table_path
        # with open(table.full_path, mode='wt', encoding='utf-8', newline='') as out_file:
        #     writer = csv.DictWriter(out_file, fieldnames=['timestamp'])
        #     writer.writeheader()
        #     writer.writerow({"timestamp": datetime.now().isoformat()})

        # Save table manifest (output.csv.manifest) from the tabledefinition
        self.write_manifest(table)

        # Write new state - will be available next run
        # self.write_state_file({"some_state_parameter": "value"})

        # ####### EXAMPLE TO REMOVE END

    def check_output_files(self, file_descriptors: list) -> tuple:
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
        # for key, file in out_files.items():
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
                    # TODO: Append account_id at the end of the row
                    row.insert(0, account_id)
                    writer.writerow(row)
            pass

    @sync_action('load_accounts')
    def load_accounts(self):
        accounts = self.client.get_accounts()
        result = [SelectElement(value=acc['id'], label=f"{acc['name']} ({acc['id']})") for acc in accounts]
        return result

    @sync_action('list_templates')
    def list_templates(self):
        all_templates = []
        for account_id in self.configuration.parameters.get('accounts'):
            account_templates = self.client.get_templates(account_id=account_id)
            all_templates.extend(account_templates)
        return all_templates

    @sync_action('test_debug')
    def list_report_columns(self):
        # TODO: This is just a demo

        body = {
            "start_date": "2020-12-20",
            "end_date": "2020-12-20",
            "granularity": "TOTAL",
            "columns": [
                "CAMPAIGN_NAME", "SPEND_IN_MICRO_DOLLAR"
            ],
            "level": "CAMPAIGN",
            "report_format": "JSON"
        }

        result = [SelectElement(value=col['id']) for col in [{'id': 'Column1'}, {'id': 'Column2'}, {'id': 'Column3'}]]
        return result


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()

        out_dir = 'C:\\Users\\DK\\Revolt\\Projects\\kds-team.ex-pinterest\\data\\out\\tables\\fds'
        of = {
            '549765938918': 'C:\\Users\\DK\\Revolt\\Projects\\kds-team.ex-pinterest\\data\\out\\files\\549765938918.raw.csv'
        }
        # comp.combine_output_files(out_dir, of)

        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
