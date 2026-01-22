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
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        self.sensor_report = 0
        self.avg_t = []

    def process_batch(self, data_batch: List[tuple]) -> str:
        try:
            if (isinstance(data_batch, List) is False):
                raise Exception(f"Error data is not a list, data type -> \
{type(data_batch)}")

            for data in data_batch:
                if (isinstance(data, tuple) is False or data[0]
                   not in ["temp", "humidity", "presure"]):
                    raise Exception("Error, invalide data type")
                float(data[1])
                self.sensor_report += 1
                if (data[0] == "temp"):
                    self.avg_t.append(data[1])

        except (Exception, ValueError) as e:
            print(e)
            return "0 readings"
        else:
            return f"{self.sensor_report} readings"

    def filter_data(self, data_batch: List[Union[tuple, str]],
                    criteria: Optional[str] = None) -> List[Any]:
        return [data for data in data_batch if data[0] in
                ["temp", "humidity", "presure"]]

    def get_stats(self):
        return sum(self.avg_t) / len(self.avg_t)


class TransactionStream(DataStream):
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        self.operation = 0
        self.net_flow = []

    def process_batch(self, data_batch: List[tuple]) -> str:
        try:
            if (isinstance(data_batch, List) is False):
                raise Exception(f"Error data is not a list, data type -> \
{type(data_batch)}")

            for data in data_batch:
                if (isinstance(data, tuple) is False or data[0]
                   not in ["buy", "sell"]):
                    raise Exception("Error, invalide data type")
                int(data[1])
                self.operation += 1
                if (data[0] == "sell"):
                    self.net_flow.append(data[1] * -1)
                else:
                    self.net_flow.append(data[1])

        except (Exception, ValueError) as e:
            print(e)
            return "0 readings"
        else:
            return f"{self.operation} operation"

    def filter_data(self, data_batch: List[Union[tuple, str]], criteria: Optional[str] = None):
        return [data for data in data_batch if data[0] in ["buy", "sell"]]

    def get_stats(self):
        result = 0
        for operation in self.net_flow:
            result += operation
        if (result > 0):
            return f"+{result}"
        return str(result)


class EventStream(DataStream):
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        self.events = 0
        self.nb_errors = 0

    def process_batch(self, data_batch: List[str]) -> str:
        try:
            if (isinstance(data_batch, List) is False):
                raise Exception(f"Error data is not a list, data type -> \
{type(data_batch)}")

            for data in data_batch:
                if (isinstance(data, str) is False or data
                   not in ["error", "login", "logout"]):
                    raise Exception("Error, invalide data type")
                self.events += 1
                if (data == "error"):
                    self.nb_errors += 1

        except (Exception, ValueError) as e:
            print(e)
            return "0 readings"
        else:
            return f"{self.events} events"

    def filter_data(self, data_batch: List[Union[tuple, str]], criteria: Optional[str] = None):
        return [data for data in data_batch if (isinstance(data, str) and data in ["error", "login", "logout"])]

    def get_stats(self):
        return self.nb_errors


class StreamProcessor(DataStream):


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
    print(f"Processing sensor batch: [{"".join(f"{x}:{y}, " for x, y in data_batch_filtered)}]")
    print(f"Sensor analysis: {sensor_stream.process_batch(data_batch_filtered)} processed, avg tem: {sensor_stream.get_stats()}°C")

    print("\nInitializing Transaction Stream ...")
    transaction_stream = TransactionStream("TRANS_001")
    print(f"Stream ID: {transaction_stream.stream_id}, Type: Financial data")
    data_batch_filtered = transaction_stream.filter_data(data_batch)
    print(f"Processung transaction batch: [{"".join(f"{x}:{y}, " for x, y in data_batch_filtered)}]")
    print(f"Transaction analysisi: {transaction_stream.process_batch(data_batch_filtered)} processed, net flow: {transaction_stream.get_stats()} units")

    print("\nInitializing Event Stream...")
    event_stream = EventStream("EVENT_001")
    print(f"Stream ID: {event_stream.stream_id}, Type: System Events")
    data_batch_filtered = event_stream.filter_data(data_batch)
    print(f"Processung event batch: [{"".join(f"{event}, " for event in data_batch_filtered)}]")
    print(f"Event analysisi: {event_stream.process_batch(data_batch_filtered)} processed, {event_stream.get_stats()} error detected")

    print("\n=== Polymorphic Stream Processing ===\n")
    print("processing mixed stream types through unified interface ...")
