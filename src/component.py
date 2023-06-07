"""
Template Component main class.

"""
import csv
import logging
from datetime import datetime

from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from Pinterest.client import PinterestClient


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
        # self.cfg: Configuration
        self._pinterest_client: PinterestClient = None

    @property
    def client(self):
        if not self._pinterest_client:
            api_token = self.configuration.parameters.get('#api_token')
            self._pinterest_client = PinterestClient(api_token)
        return self._pinterest_client

    def run(self):
        """
        Main execution code
        """

        # params = self.configuration.parameters

        # get last state data/in/state.json from previous run
        previous_state = self.get_state_file()
        logging.info(previous_state.get('some_state_parameter'))

        # Create output table (Tabledefinition - just metadata)
        table = self.create_out_table_definition('output.csv', incremental=True, primary_key=['timestamp'])

        # get file path of the table (data/out/tables/Features.csv)
        out_table_path = table.full_path
        logging.info(out_table_path)

        # DO whatever and save into out_table_path
        with open(table.full_path, mode='wt', encoding='utf-8', newline='') as out_file:
            writer = csv.DictWriter(out_file, fieldnames=['timestamp'])
            writer.writeheader()
            writer.writerow({"timestamp": datetime.now().isoformat()})

        # Save table manifest (output.csv.manifest) from the tabledefinition
        self.write_manifest(table)

        # Write new state - will be available next run
        self.write_state_file({"some_state_parameter": "value"})

        # ####### EXAMPLE TO REMOVE END

    @sync_action('load_accounts')
    def load_accounts(self):
        accounts = self.client.get_accounts()
        result = [SelectElement(value=acc['id'], label=f"{acc['name']} ({acc['id']})") for acc in accounts]
        return result


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

        clie

        result = [SelectElement(value=col['id']) for col in [{'id': 'Column1'}, {'id': 'Column2'}, {'id': 'Column3'}]]
        return result


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
