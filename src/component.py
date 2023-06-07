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
        self.pinterest_client: PinterestClient

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
        self._init_pinterest_client()
        accounts = self.pinterest_client.get_accounts()
        result = [SelectElement(value=acc['id'], label=f"{acc['name']} ({acc['id']})") for acc in accounts]
        return result

    @sync_action('list_report_columns')
    def list_report_columns(self):
        # TODO: This is just a demo
        result = [SelectElement(value=col['id']) for col in [{'id': 'Column1'}, {'id': 'Column2'}, {'id': 'Column3'}]]
        return result

    def _init_pinterest_client(self):
        api_token = self.configuration.parameters.get('#api_token')
        client = PinterestClient(api_token)
        self.pinterest_client = client


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
