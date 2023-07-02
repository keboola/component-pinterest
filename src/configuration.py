from dataclasses import dataclass, field
import dataconf
from enum import Enum

from pyhocon.config_tree import ConfigTree

_keys = {
    "Ad ID",
    "Ad group ID",
    "Advertiser",
    "Campaign ID",
    "Date",
    "Keyword ID",
    "Organic pin ID",
    "Product group ID",
    "Targeting Type"
}


def retrieve_keys(column_list: list):
    keys = [item for item in column_list if item in _keys]
    return keys


class LevelEnum(Enum):
    ADVERTISER = "ADVERTISER"
    ADVERTISER_TARGETING = "ADVERTISER_TARGETING"
    CAMPAIGN = "CAMPAIGN"
    CAMPAIGN_TARGETING = "CAMPAIGN_TARGETING"
    AD_GROUP = "AD_GROUP"
    AD_GROUP_TARGETING = "AD_GROUP_TARGETING"
    PIN_PROMOTION = "PIN_PROMOTION"
    PIN_PROMOTION_TARGETING = "PIN_PROMOTION_TARGETING"
    KEYWORD = "KEYWORD"
    PRODUCT_GROUP = "PRODUCT_GROUP"
    PRODUCT_GROUP_TARGETING = "PRODUCT_GROUP_TARGETING"
    PRODUCT_ITEM = "PRODUCT_ITEM"


class DaysEnum(Enum):
    D0 = "0"
    D1 = "1"
    D7 = "14"
    D30 = "30"
    D60 = "60"


class GranularityEnum(Enum):
    TOTAL = "TOTAL"
    DAY = "DAY"
    HOUR = "HOUR"
    WEEK = "WEEK"
    MONTH = "MONTH"


class ConversionReportTimeEnum(Enum):
    TIME_OF_AD_ACTION = "TIME_OF_AD_ACTION"
    TIME_OF_CONVERSION = "TIME_OF_CONVERSION"


@dataclass
class Destination:
    table_name: str
    incremental_loading: bool = True
    primary_key: list[str] = None
    primary_key_existing: list[str] = None


@dataclass
class TimeRange:
    granularity: GranularityEnum
    date_from: str = ""
    date_to: str = ""


@dataclass
class ReportSettings:
    level: LevelEnum
    columns: list[str]
    click_window_days: str
    engagement_window_days: str
    view_window_days: str
    conversion_report_time: ConversionReportTimeEnum


class ConfigurationBase:

    @staticmethod
    def fromDict(parameters: dict):
        return dataconf.dict(parameters, Configuration, ignore_unexpected=True)
        pass


@dataclass
class Configuration(ConfigurationBase):
    input_variant: str
    accounts: list[str]
    destination: Destination
    time_range: TimeRange
    report_specification: ReportSettings = field(default_factory=lambda: ConfigTree({}))
    existing_report_ids: list[str] = field(default_factory=lambda: ConfigTree({}))
    debug: bool = False


if __name__ == '__main__':
    json_conf_1 = """
    {
    "#api_token": "pina_AEA5FJQWAATGQAQAGDAJODLWPKHK5CABACGSOWK2SH6L5HANXBIT7FOUTVETMSUT7KUY7SA36HAQGWG4NRQZYC3I7QBXRPYA",
    "debug": false,
    "accounts": ["jedna", "dve", "tri"],
    "time_range": {
      "date_to": "today - 5",
      "date_from": "today",
      "granularity": "HOUR"
    },
    "destination": {
      "table_name": "fds",
      "primary_key": [],
      "selected_variant": "report_specification",
      "incremental_loading": true
    },
    "input_variant": "report_specification",
    "report_specification": {
      "level": "CAMPAIGN_TARGETING",
      "columns": ["CAMPAIGN_NAME", "SPEND_IN_DOLLAR"],
      "view_window_days": "1",
      "click_window_days": "30",
      "conversion_report_time": "TIME_OF_AD_ACTION",
      "engagement_window_days": "30"
    }
  }
    """

    cf1 = dataconf.loads(json_conf_1, Configuration, ignore_unexpected=True)

    pass
