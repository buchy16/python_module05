from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        pass


class SensorStream(DataStream):
    def __init__(self, stream_id:str):
        self.stream_id = stream_id
        self.__sensor_report = 0
        self.__avg_t = []

    def process_batch(self, data_batch: List[tuple]) -> str:
        try:
            if (isinstance(data_batch, List) is False):
                raise Exception(f"Error data is not a list, data type -> \
{type(data_batch)}")

            for data in data_batch:
                if (isinstance(data, tuple) is False or data[0] not in
                    ["temp", "humidity", "pressure"]):
                    raise Exception(f"Error, invalide data type")
                float(data[1])
                self.__sensor_report += 1
                self.__avg_t.append(data[1])

        except (Exception, ValueError) as e:
           print(e)
           return "0 readings"
        else:
            return f"{self.__sensor_report} readings"

    def filter_data(self, data_batch: List[tuple], 
                    criteria: Optional[str] = None):
        return [data for data in data_batch if data[0] in 
                ["temp", "humidity", "pressure"]]

    def get_stats(self):
        return sum(self.__avg_t) / len(self.__avg_t)


class TransactionStream(DataStream):
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        self.__operation = 0
        self.__net_flow = 0


class EventStream(DataStream):
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        self.events = 0
        self.nb_errors = 0


# class StreamProcessor(DataStream):


if (__name__ == "__main__"):
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    data_batch = [
                ("temp", 22.5), ("humidity", 65), ("presure", 1013),
                ("buy", 100), ("sell", 150), ("buy", 75),
                "login", "error", "logout"
                ]

    print("Initializing Sensor Stream...")
    sensor_stream = SensorStream("SENSOR_001")
    print(f"Stream ID: {sensor_stream.stream_id}, Type: Environmental Data")
    data_batch_filtered = sensor_stream.filter_data(data_batch)
    data_batch_stats = sensor_stream.get_stats()
    print(f"Processing sensor batch: {data_batch_filtered}")
    print(f"Sensor analysis: {sensor_stream.process_batch(data_batch_filtered)} processed, avg tem: {data_batch_stats}")
