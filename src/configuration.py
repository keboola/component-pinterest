import dataclasses
from dataclasses import dataclass, field
from enum import Enum
from typing import List

import dataconf
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
    """Select keys out of a list of columns

    Args:
        column_list: List of column names

    Returns:
        List of columns that belong to primary key set (dimensions)
    """
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


@dataclass
class TimeRange:
    granularity: GranularityEnum
    date_from: str = ""
    date_to: str = ""


@dataclass
class ReportSettings:
    level: LevelEnum = "ADVERTISER"
    columns: list[str] = field(default_factory=lambda: ConfigTree({}))
    conversion_window: str = "30/30/30"
    conversion_report_time: ConversionReportTimeEnum = "TIME_OF_AD_ACTION"

    @property
    def click_window_days(self):
        return self.conversion_window.split('/')[0]

    @property
    def engagement_window_days(self):
        return self.conversion_window.split('/')[1]

    @property
    def view_window_days(self):
        return self.conversion_window.split('/')[2]


class ConfigurationBase:

    @staticmethod
    def fromDict(parameters: dict):
        return dataconf.dict(parameters, Configuration, ignore_unexpected=True)
        pass

    @staticmethod
    def _convert_private_value_inv(value: str):
        if value and value.startswith('pswd_'):
            return value.replace('pswd_', '#', 1)
        else:
            return value

    @classmethod
    def get_dataclass_required_parameters(cls) -> List[str]:
        """
        Return list of required parameters based on the dataclass definition (no default value)
        Returns: List[str]

        """
        return [cls._convert_private_value_inv(f.name) for f in dataclasses.fields(cls)
                if f.default == dataclasses.MISSING
                and f.default_factory == dataclasses.MISSING]


@dataclass
class Configuration(ConfigurationBase):
    input_variant: str
    accounts: list[str]
    destination: Destination
    time_range: TimeRange
    report_specification: ReportSettings = field(default_factory=lambda: ConfigTree({}))
    existing_report_ids: list[str] = field(default_factory=lambda: ConfigTree({}))
    debug: bool = False
    enable_advertiser_columns: bool = False
