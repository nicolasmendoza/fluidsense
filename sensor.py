import logging
from typing import Dict, List

from pandas import read_csv, isna, notna, DataFrame, concat


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Config:
    CSV_PATH = "sensor.csv"
    DATE_MIN = "2018-04-01"
    DATE_MAX = "2018-04-30"
    SENSOR_VALUE_MIN = 20
    SENSOR_VALUE_MAX = 30
    SENSOR_COLUMNS = ["sensor_07", "sensor_47"]


class SensorDataSet:
    """
    Class to load, filter, and provide access to sensor data based in CSV.
    """
    def __init__(self, config: Config):
        self.config = config
        self.dataframe = self.load_and_filter()

    def load_and_filter(self) -> DataFrame:
        """
        Loads the CSV file and apply filtering based on the filter configuration,
        returning a Dataframe filtered.
        """
        raw_data = read_csv(self.config.CSV_PATH, parse_dates=['timestamp'])
        filtered_data = raw_data[
            (raw_data['timestamp'] >= self.config.DATE_MIN) & (raw_data['timestamp'] <= self.config.DATE_MAX)]

        sensor_columns = self.config.SENSOR_COLUMNS

        records = []
        for index, row in filtered_data.iterrows():
            timestamp, status = row['timestamp'], row['machine_status']
            if isna(timestamp):
                continue
            records.extend(self.process_row(row, sensor_columns, status))

        return DataFrame(records)

    def process_row(self, row: Dict, sensor_columns: List[str], status: str) -> List[Dict]:
        """Processes a single row of the raw data."""
        return [{'Fecha': row['timestamp'].date().isoformat(),
                 'Hora': row['timestamp'].time().isoformat(),
                 'Sensor': sensor,
                 'Medicion': measurement,
                 'Estado': status}
                for sensor in sensor_columns if notna(measurement := row[sensor])
                and self.config.SENSOR_VALUE_MIN < measurement < self.config.SENSOR_VALUE_MAX]

    def get_data(self) -> list[dict]:
        """
        Gets the processed data as a dictionary based on a Datafram.
        """
        return self.dataframe.to_dict(orient='records')

    def add_data(self, data):
        """
        Adds new data to the existing DataFrame.
        """
        data_as_dicts = [measurement.model_dump() for measurement in data]
        new_data = DataFrame.from_records(data_as_dicts)
        self.dataframe = concat([self.dataframe, new_data] if self.dataframe is not None else [new_data], ignore_index=True)
        self.dataframe.reset_index(drop=True, inplace=True)
        logger.info(self.dataframe)


def get_sensor_dataset():
    """
    Returns an instance of the SensorDataset with the default configuration.
    """
    return SensorDataSet(Config())
