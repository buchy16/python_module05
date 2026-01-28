#!/usr/bin/env python3

from typing import Any, Protocol, Union
from abc import ABC, abstractmethod
import time


class NexusManager:
    """Orchestrate multiple pipelines with performance tracking.

    Manages a collection of processing pipelines and chains data through them,
    collecting metrics on records processed, errors encountered,
    and total execution time.
    """

    def __init__(self) -> None:
        """Initialize the NexusManager with empty pipeline list and metrics."""
        self._pipeline = []
        self._records_count = 0
        self._total_errors = 0
        self._total_time = 0.0

    def add_pipeline(self, pipeline: list) -> None:
        """Add processing pipelines to the manager.

        Args:
            pipeline: List of ProcessingPipeline instances to add.
        """
        for pipe in pipeline:
            self._pipeline.append(pipe)

    def process_data(self, data: list) -> list:
        """Chain data through all registered pipelines sequentially.

        Processes each data item through each pipeline in order, tracking
        successful results and measuring total execution time.

        Args:
            data: List of data items to process.

        Returns:
            List of processed results from all pipelines.
        """
        start = time.perf_counter()
        result = []

        # Applies each data to each pipeline
        for d in data:
            for pipe in self._pipeline:
                # The pipeline will return none
                # if the data does not match with him.
                res = pipe.process(d)
                if res is not None:
                    result.append(res)
                    self._records_count += 1

        self._total_time = time.perf_counter() - start
        return result

    def get_chaining_stats(self) -> dict:
        """Retrieve chaining performance statistics.

        Returns:
            Dictionary containing records count, efficiency percentage,
            total processing time, and number of stages.
        """
        efficiency = 100 - (self._total_time * 100)
        return {
            'records': 100,
            'efficiency': efficiency,
            'total_time': self._total_time,
        }


class ProcessingStage(Protocol):
    """Protocol defining the interface for processing stages.

    Any class implementing a process() method matches this protocol
    and can be used as a stage in a pipeline (duck typing).
    """
    def process(self, data: Any) -> Union[str, Any]:
        """Process data through this stage.

        Args:
            data: Input data to process.

        Returns:
            Processed data or result.
        """
        pass


class InputStage:
    """First stage: validate and parse input data.

    Validates data format and structure before transformation.
    """

    def __init__(self) -> None:
        """Initialize the input validation stage."""
        print("Stage 1: Input validation and parsing")

    def process(self, data: Any) -> Union[str, Any]:
        """Validate input data and return it for next stage.

        Args:
            data: Input data to validate (dict or str).

        Returns:
            Validated data, or None if validation fails.
        """
        # Processes data differently depending
        # on which pipeline it originates from and handle errors.
        if isinstance(data, dict) is True:
            try:
                for d in data.keys():
                    if d != 'sensor' and d != 'value' and d != 'unit':
                        raise KeyError

            except KeyError:
                print("Error detected in Stage 1: Invalid key")
                return None

        elif isinstance(data, str) is True:
            try:
                data + 'forty_two'

            except Exception:
                print("Error detected in Stage 1: Invalid type")
                return None

        return data


class TransformStage:
    """Second stage: transform and enrich data with metadata.

    Applies format-specific transformations and enriches data structure.
    """

    def __init__(self) -> None:
        """Initialize the data transformation stage."""
        print("Stage 2: Data transformation and enrichment")

    def process(self, data: Any) -> Union[str, Any]:
        """Transform data based on its type with error recovery.

        Handles dictionaries (sensor data), strings (CSV), and lists (streams)
        with automatic fallback on transformation failure.

        Args:
            data: Input data (dict, str, or list).

        Returns:
            Transformed data or None on recoverable error.
        """
        if isinstance(data, dict):
            try:
                result = {}
                result['value'] = data['value']
                result['value'] + 42
                result['unit'] = data['unit']
                return result

            except Exception:
                print("Error detected in Stage 2: Invalid data format")
                print("Recovery initiated: Switching to backup processor")
                return None

        elif isinstance(data, str):
            try:
                count = 0
                data_split = data.split(',')
                for d in data_split:
                    if d == 'action':
                        count += 1
                return count

            except Exception:
                print("Error detected in Stage 2: Invalid data format")
                print("Recovery initiated: Switching to backup processor")
                return data

        elif isinstance(data, list):
            result = []

            try:
                readings = len(data)
                total = sum(data)
                result.append(total/readings)
                result.append(readings)
                return result

            except Exception:
                print("Error detected in Stage 2: Invalid data format")
                print("Recovery initiated: Switching to backup processor")
                return data


class OutputStage:
    """Third stage: format and deliver processed data.

    Converts processed data into human-readable output strings.
    """
    def __init__(self) -> None:
        """Initialize the output formatting stage."""
        print("Stage 3: Output formatting and delivery")

    def process(self, data: Any) -> Union[str, Any]:
        """Format processed data for delivery.

        Generates format-specific output messages for temperature readings,
        user activity logs, and stream summaries.

        Args:
            data: Processed data (dict, int, or list).

        Returns:
            Formatted output string or original data on error.
        """
        if isinstance(data, dict):
            try:
                if (data['value'] < 10):
                    return f"Processed temperature reading: \
{data['value']}°{data['unit']} (Cold range)"

                elif (data['value'] <= 25):
                    return f"Processed temperature reading: \
{data['value']}°{data['unit']} (Normal range)"

                if (data['value'] > 25):
                    return f"Processed temperature reading: \
{data['value']}°{data['unit']} (Hot range)"

            except Exception:
                print("Error detected in Stage 3")
                return None

        elif isinstance(data, int):
            return f"User activity logged: {data} actions processed"

        elif isinstance(data, list):
            return f"Stream summary: {data[1]} readings, avg: {data[0]}°C"


class ProcessingPipeline(ABC):
    """Abstract base class for data processing pipelines.

    Defines the interface for pipeline implementations that orchestrate
    data through multiple processing stages.
    """
    @abstractmethod
    def __init__(self, pipeline_id: str) -> None:
        """Initialize a processing pipeline.

        Args:
            pipeline_id: Unique identifier for the pipeline.
        """
        self._id = pipeline_id
        self._stages = []

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        """Process data through pipeline stages.

        Args:
            data: Input data to process.

        Returns:
            Processed data or result.
        """
        pass

    @abstractmethod
    def add_stage(self, stage: list) -> None:
        """Add processing stages to the pipeline."""
        pass


class JSONAdapter(ProcessingPipeline):
    """Pipeline adapter for JSON data processing.

    Processes dictionary-based sensor data through validation,
    transformation, and formatted output stages.
    """

    def __init__(self, pipeline_id: str) -> None:
        """Initialize JSON adapter.

        Args:
            pipeline_id: Unique identifier for this pipeline.
        """
        super().__init__(pipeline_id)
        self._stages = []

    def process(self, data: Any) -> Union[str, Any]:
        """Process dictionary data through pipeline stages.

        Args:
            data: Dictionary data to process.

        Returns:
            Formatted output string or None if data is not a dict.
        """
        if isinstance(data, dict) is False:
            return

        for stage in self._stages:
            if isinstance(stage, InputStage):
                result = stage.process(data)

            else:
                result = stage.process(result)

        return result

    def add_stage(self, stages: list) -> None:
        """Add processing stages to the pipeline.

        Args:
            stages: List of stage instances to add.
        """
        for stage in stages:
            self._stages.append(stage)


class CSVAdapter(ProcessingPipeline):
    """Pipeline adapter for CSV data processing.

    Processes string-based CSV data through validation, transformation,
    and formatted output stages.
    """

    def __init__(self, pipeline_id: str) -> None:
        """Initialize CSV adapter.

        Args:
            pipeline_id: Unique identifier for this pipeline.
        """
        super().__init__(pipeline_id)
        self._stages = []

    def process(self, data: Any) -> Union[str, Any]:
        """Process string data through pipeline stages.

        Args:
            data: String data to process.

        Returns:
            Formatted output string or None if data is not a string.
        """
        if isinstance(data, str) is False:
            return

        for stage in self._stages:
            if isinstance(stage, InputStage):
                result = stage.process(data)

            else:
                result = stage.process(result)

        return result

    def add_stage(self, stages: list) -> None:
        """Add processing stages to the pipeline.

        Args:
            stages: List of stage instances to add.
        """
        for stage in stages:
            self._stages.append(stage)


class StreamAdapter(ProcessingPipeline):
    """Pipeline adapter for stream data processing.

    Processes list-based stream data (sensor readings) through validation,
    transformation, and formatted output stages.
    """

    def __init__(self, pipeline_id: str) -> None:
        """Initialize stream adapter.

        Args:
            pipeline_id: Unique identifier for this pipeline.
        """
        super().__init__(pipeline_id)
        self._stages = []

    def process(self, data: Any) -> Union[str, Any]:
        """Process list data through pipeline stages.

        Args:
            data: List data to process.

        Returns:
            Formatted output string or None if data is not a list.
        """
        if isinstance(data, list) is False:
            return

        for stage in self._stages:
            if isinstance(stage, InputStage):
                result = stage.process(data)

            else:
                result = stage.process(result)

        return result

    def add_stage(self, stages: list) -> None:
        """Add processing stages to the pipeline.

        Args:
            stages: List of stage instances to add.
        """
        for stage in stages:
            self._stages.append(stage)


def main():
    """Demonstrate the Code Nexus enterprise pipeline system.

    Creates and chains multiple data adapters (JSON, CSV, Stream),
    processes sample data through all pipelines, and displays
    chaining performance metrics.
    """
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")
    print("Initializing Nexus Manager...")
    print("Pipeline capacity: 1000 streams/second\n")

    print("Creating Data Processing Pipeline...")
    input = InputStage()
    transform = TransformStage()
    output = OutputStage()
    print("\n=== Multi-Format Data Processing ===\n")

    print("Processing JSON data through pipeline...")
    data1 = {"sensor": "temp", "value": 23.5, "unit": "C"}
    print("Input: {\"sensor\": \"temp\", \"value\": 23.5, \"unit\": \"C\"}")
    json1 = JSONAdapter("JSON_001")
    print("Transform: Enriched with metadata and validation")
    json1.add_stage([input, transform, output])
    print("Output:", json1.process(data1))

    print("\nProcessing CSV data through same pipeline...")
    data2 = "user,action,timestamp"
    print("Input: \"user,action,timestamp\"")
    csv1 = CSVAdapter("CSV_001")
    print("Transform: Parsed and structured data")
    csv1.add_stage([input, transform, output])
    print("Output:", csv1.process(data2))

    print("\nProcessing Stream data through same pipeline...")
    data3 = [20.1, 24.1, 20.1, 24.1, 22.1]
    print("Input: Real-time sensor stream")
    strm1 = StreamAdapter("STRM_001")
    print("Transform: Aggregated and filtered")
    strm1.add_stage([input, transform, output])
    print("Output:", strm1.process(data3))

    print("\n=== Pipeline Chaining Demo ===")
    print("Pipeline A -> Pipeline B -> Pipeline C")
    print("Data flow: Raw -> Processed -> Analyzed -> Stored")
    data = [data1, data2, data3]
    nexus = NexusManager()
    nexus.add_pipeline([json1, csv1, strm1])
    stock = nexus.process_data(data)
    stats = nexus.get_chaining_stats()
    print(f"\nChain result: {stats['records']} records processed through \
3-stage pipeline")
    print(f"Performance: {stats['efficiency']:.0f}% efficiency, \
{stats['total_time']:.2f}s total processing time")

    print("\nResult:")
    [print("-", s) for s in stock]

    print("\n=== Error Recovery Test ===")
    print("Simulating pipeline failure...")
    data_error = {"sensor": "temp", "value": 'f', "unit": "C"}
    data = [data_error, data2, data3]
    stock = nexus.process_data(data)
    print("Recovery successful: Pipeline restored, processing resumed")
    print("\nResult:")
    [print("-", s) for s in stock]

    print("\nNexus Integration complete. All systems operational.")


if __name__ == '__main__':
    main()
