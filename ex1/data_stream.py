from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    def __init__(self, stream_id: str, type: str):
        self.stream_id = stream_id
        self.type = type

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        pass


class SensorStream(DataStream):
    def __init__(self, stream_id: str, type: str):
        super().__init__(stream_id, type)
        self.sensor_report = 0
        self.avg_t = []

    def process_batch(self, data_batch: List[Any]) -> str:
        data_f = self.filter_data(data_batch)

        try:
            if (isinstance(data_f, List) is False):
                raise Exception(f"Error data is not a list, data type -> \
{type(data_f)}")

            for data in data_f:
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
        filtered_data = [data for data in data_batch if data[0] in
                         ["temp", "humidity", "presure"]]

        if (criteria == "High-priority"):
            for data in filtered_data:
                if (data[0] == "temp" and (data[1] > -15 and data[1] < 35)):
                    filtered_data.remove(data)
                elif (data[0] == "humidity" and (data[1] > 10 and data[1] < 90)):
                    filtered_data.remove(data)
                elif (data[0] == "presure" and (data[1] > 1005 and data[1] < 1025)):
                    filtered_data.remove(data)
        return filtered_data

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {"average_temperature": sum(self.avg_t) / len(self.avg_t)}


class TransactionStream(DataStream):
    def __init__(self, stream_id: str, type: str):
        super().__init__(stream_id, type)
        self.operation = 0
        self.net_flow = []

    def process_batch(self, data_batch: List[Any]) -> str:
        data_f = self.filter_data(data_batch)
        try:
            if (isinstance(data_f, List) is False):
                raise Exception(f"Error data is not a list, data type -> \
{type(data_f)}")

            for data in data_f:
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
        filtered_data = [data for data in data_batch if data[0] in ["buy", "sell"]]

        print("debug1 ==============", filtered_data)
        if (criteria == "High-priority"):
            for data in filtered_data:
                if data[0] == "sell" or (data[0] == "buy" and data[1] < 10_000_000):
                    filtered_data.remove(data)
            
            print("debug2 ==============", filtered_data)
        return filtered_data

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        result = 0
        for operation in self.net_flow:
            result += operation
        if (result > 0):
            return {"net_flow": f"+{result}"}
        return {"net_flow": str(result)}


class EventStream(DataStream):
    def __init__(self, stream_id: str, type: str):
        super().__init__(stream_id, type)
        self.events = 0
        self.nb_errors = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        data_f = self.filter_data(data_batch)

        try:
            if (isinstance(data_f, List) is False):
                raise Exception(f"Error data is not a list, data type -> \
{type(data_f)}")

            for data in data_f:
                if (isinstance(data, str) is False or data
                   not in ["error", "login", "logout", "emtpy_trash"]):
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
        filtered_data = [data for data in data_batch if (isinstance(data, str) and data in ["error", "login", "logout", "emtpy_trash"])]

        if (criteria == "High-priority"):
            for data in filtered_data:
                if (data != "error"):
                    filtered_data.remove(data)
        return (filtered_data)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {"error_count": self.nb_errors}


class StreamProcessor():
    def process_batch(self, data_batch: List[Any], streams: List[object]):
        for stream in streams:
            result = stream.process_batch(data_batch)
            print(f"- {stream.type} data: {result} processed")

    def process_batch_filtered(self, data_batch: List[Any], streams: List[Any], criteria: str) -> Dict[str, int]:
        liste = []
        for stream in streams:
            liste.append(len(stream.filter_data(data_batch, criteria)))

        return {key.type: value for key, value in zip(streams, liste)}


if (__name__ == "__main__"):
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    data_batch = [
                ("temp", 22.5), ("humidity", 65), ("presure", 1013),
                ("buy", 100), ("sell", 150), ("buy", 75),
                "login", "error", "logout"
                ]

    data_batch2 = [
                ("temp", -33), ("humidity", 20),
                ("buy", 100), ("sell", 150), ("buy", 15000000), ("buy", 1),
                "login", "emtpy_trash", "logout"
                ]

    data_batch3 = [
                ("temp", -33), ("humidity", 95),
                ("buy", 100), ("sell", 150), ("buy", 15000000), ("buy", 1),
                "login", "emtpy_trash", "logout"
                ]

    stream_type = [
                   SensorStream("SENSOR_002", "Sensor"),
                   TransactionStream("TRANS_002", "Transaction"),
                   EventStream("EVENT_001", "Event")
                   ]


    print("Initializing Sensor Stream...")
    sensor_stream = SensorStream("SENSOR_001", "Sensor")
    print(f"Stream ID: {sensor_stream.stream_id}, Type: Environmental Data")
    data_batch_filtered = sensor_stream.filter_data(data_batch)
    print(f"Processing sensor batch: [{''.join(f'{x}:{y}, ' for x, y in data_batch_filtered)}]")
    print(f"Sensor analysis: {sensor_stream.process_batch(data_batch)} processed, avg tem: {sensor_stream.get_stats()['average_temperature']}°C")

    print("\nInitializing Transaction Stream ...")
    transaction_stream = TransactionStream("TRANS_001", "Transaction")
    print(f"Stream ID: {transaction_stream.stream_id}, Type: Financial data")
    data_batch_filtered = transaction_stream.filter_data(data_batch)
    print(f"Processing transaction batch: [{''.join(f'{x}:{y}, ' for x, y in data_batch_filtered)}]")
    print(f"Transaction analysisi: {transaction_stream.process_batch(data_batch)} processed, net flow: {transaction_stream.get_stats()['net_flow']} units")

    print("\nInitializing Event Stream...")
    event_stream = EventStream("EVENT_001", "Event")
    print(f"Stream ID: {event_stream.stream_id}, Type: System Events")
    data_batch_filtered = event_stream.filter_data(data_batch)
    print(f"Processing event batch: [{''.join(f'{event}, ' for event in data_batch_filtered)}]")
    print(f"Event analysisi: {event_stream.process_batch(data_batch)} processed, {event_stream.get_stats()['error_count']} error detected")

    print("\n=== Polymorphic Stream Processing ===")
    print("processing mixed stream types through unified interface ...\n")
    print("Batch 1 Results:")
    StreamProcessor().process_batch(data_batch2, stream_type)

    print("\n Sream filtering active: High-priority data only")
    filtered_result = StreamProcessor().process_batch_filtered(data_batch3, stream_type, "High-priority")
    print(f"Filtered results: {filtered_result['Sensor']} critical sensor alerts, {filtered_result['Transaction']} large transaction")
