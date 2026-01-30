"""Polymorphic data stream system.

Includes stream implementations for sensors, transactions, and events,
plus a processor to orchestrate them through a unified interface.

credit:
all docstring made by IA
"""

from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    """Abstract interface defining a data stream."""
    def __init__(self, stream_id: str, type: str):
        """Initialize a generic data stream.

        Args:
            stream_id: Unique identifier for the stream.
            type: Human-readable stream type (e.g., "Sensor").
        """
        self.stream_id = stream_id
        self.type = type

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a data batch and return a textual summary."""
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        """Filter batch items using an optional criterion."""
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return aggregated statistics for the stream."""
        pass


class SensorStream(DataStream):
    """Stream for sensor data (temperature, humidity, pressure)."""
    def __init__(self, stream_id: str, type: str):
        """Initialize counters and measurement accumulators."""
        super().__init__(stream_id, type)
        self.sensor_report = 0
        self.avg_t = []

    def process_batch(self, data_batch: List[Any]) -> str:
        """Validate, filter, and aggregate a batch of sensor readings.

        Returns a string like "N readings" or "0 readings" if validation
        or filtering fails.
        """
        try:
            if (isinstance(data_batch, List) is False):
                raise Exception(f"Error data is not a list, data type -> \
{type(data_batch)}")
            data_f = self.filter_data(data_batch)
            # filter the data batch to only keep sensor data

            if (len(data_f) <= 0):
                raise Exception("Error data is empty, no valid data found")

            for data in data_f:
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
                    criteria: Optional[str] = None) -> List[tuple]:
        """Keep valid sensor tuples; apply priority filter if requested."""
        filtered_data = [data for data in data_batch
                         if isinstance(data, tuple) is True
                         and data[0] in ["temp", "humidity", "presure"]]

        if (criteria == "High-priority"):
            return [data for data in filtered_data if
                    (data[0] == "temp" and (data[1] < -15 or data[1] > 35))
                    or (data[0] == "humidity"
                        and (data[1] < 10 or data[1] > 90))
                    or (data[0] == "presure"
                        and (data[1] < 1005 or data[1] > 1025))
                    ]
        return filtered_data

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Compute the average of collected temperatures."""
        try:
            return {"average_temperature": sum(self.avg_t) / len(self.avg_t)}
        except ZeroDivisionError as e:
            print(e)
            return {"average_temperature": 0}


class TransactionStream(DataStream):
    """Stream for buy/sell transactions and net flow tracking."""
    def __init__(self, stream_id: str, type: str):
        """Initialize counters and net flow tracking."""
        super().__init__(stream_id, type)
        self.operation = 0
        self.net_flow = []

    def process_batch(self, data_batch: List[Any]) -> str:
        """Validate, filter, and aggregate transactions into net flow."""
        try:
            if (isinstance(data_batch, List) is False):
                raise Exception(f"Error data is not a list, data type -> \
{type(data_batch)}")
            data_f = self.filter_data(data_batch)
            # filter the data batch to only keep transaction data

            if (len(data_f) <= 0):
                raise Exception("Error data is empty, no valid data found")

            for data in data_f:
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

    def filter_data(self, data_batch: List[Union[tuple, str]],
                    criteria: Optional[str] = None) -> List[tuple]:
        """Keep only valid (buy/sell, amount) tuples."""
        filtered_data = [data for data in data_batch
                         if isinstance(data, tuple) is True
                         and data[0] in ["buy", "sell"]]
        if (criteria == "High-priority"):
            return [data for data in filtered_data
                    if data[0] == "buy" and data[1] > 10000000]
        return filtered_data

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return cumulative net flow, formatted as a signed string."""
        result = 0
        for operation in self.net_flow:
            result += operation
        if (result > 0):
            return {"net_flow": f"+{result}"}
        return {"net_flow": str(result)}


class EventStream(DataStream):
    """Event stream (login/logout/error) with error counting."""
    def __init__(self, stream_id: str, type: str):
        """Initialize event and error counters."""
        super().__init__(stream_id, type)
        self.events = 0
        self.nb_errors = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        """Validate, filter, and count events in the batch."""
        try:
            if (isinstance(data_batch, List) is False):
                raise Exception(f"Error data is not a list, data type -> \
{type(data_batch)}")
            data_f = self.filter_data(data_batch)
            # filter the data batch to only keep event data

            if (len(data_f) <= 0):
                raise Exception("Error data is empty, no valid data found")

            for data in data_f:
                self.events += 1
                if (data == "error"):
                    self.nb_errors += 1

        except (Exception, ValueError) as e:
            print(e)
            return "0 readings"
        else:
            return f"{self.events} events"

    def filter_data(self, data_batch: List[Union[tuple, str]],
                    criteria: Optional[str] = None) -> List[str]:
        """Filter to keep only supported textual events."""
        filtered_data = [data for data in data_batch
                         if isinstance(data, str)
                         and data in ["error", "login",
                                      "logout", "emtpy_trash"]]

        if (criteria == "High-priority"):
            return [data for data in filtered_data if data == "error"]
        return (filtered_data)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return a dictionary with the number of errors encountered."""
        return {"error_count": self.nb_errors}


class StreamProcessor():
    """Unified processor applying operations for each stream."""
    def process_batch(self, data_batch: List[Any],
                      streams: List[object]) -> None:
        """Execute `process_batch` on each stream and print a summary."""
        for stream in streams:
            result = stream.process_batch(data_batch)
            print(f"- {stream.type} data: {result} processed")

    def process_batch_filtered(self, data_batch: List[Any],
                               streams: List[Any],
                               criteria: str) -> Dict[str, int]:
        """Count, per stream, items that satisfy a given criterion."""
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

    # data_batch = [
    #             1, 40, 2
    #             ]

    data_batch2 = [
                ("temp", -33), ("humidity", 20),
                ("buy", 100), ("sell", 150), ("buy", 15000000), ("buy", 1),
                "login", "emtpy_trash", "logout"
                ]

    data_batch3 = [
                ("temp", -33), ("humidity", 95), ("sell", 150),
                ("buy", 15000000), ("buy", 100), ("buy", 1),
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
    print(f"Processing sensor batch: \
[{''.join(f'{x}:{y}, ' for x, y in data_batch_filtered)}]")
    print(f"Sensor analysis: {sensor_stream.process_batch(data_batch)} \
processed, avg temp: {sensor_stream.get_stats()['average_temperature']}°C")

    print("\nInitializing Transaction Stream ...")
    transaction_stream = TransactionStream("TRANS_001", "Transaction")
    print(f"Stream ID: {transaction_stream.stream_id}, Type: Financial data")
    data_batch_filtered = transaction_stream.filter_data(data_batch)
    print(f"Processing transaction batch: \
[{''.join(f'{x}:{y}, ' for x, y in data_batch_filtered)}]")
    print(f"Transaction analysis: \
{transaction_stream.process_batch(data_batch)} \
processed, net flow: {transaction_stream.get_stats()['net_flow']} units")

    print("\nInitializing Event Stream...")
    event_stream = EventStream("EVENT_001", "Event")
    print(f"Stream ID: {event_stream.stream_id}, Type: System Events")
    data_batch_filtered = event_stream.filter_data(data_batch)
    print(f"Processing event batch: \
[{''.join(f'{event}, ' for event in data_batch_filtered)}]")
    print(f"Event analysis: {event_stream.process_batch(data_batch)} \
processed, {event_stream.get_stats()['error_count']} error detected")

    print("\n=== Polymorphic Stream Processing ===")
    print("processing mixed stream types through unified interface ...\n")
    print("Batch 1 Results:")
    StreamProcessor().process_batch(data_batch2, stream_type)

    print("\nSream filtering active: High-priority data only")
    filtered_result = StreamProcessor().process_batch_filtered(data_batch3,
                                                               stream_type,
                                                               "High-priority")
    print(f"Filtered results: {filtered_result['Sensor']} \
critical sensor alerts, {filtered_result['Transaction']} large transaction")
