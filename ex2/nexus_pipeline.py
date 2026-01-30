"""Nexus Pipeline - Enterprise Data Processing System.

This module provides a flexible pipeline architecture for processing multiple
data formats (JSON, CSV, Stream) through configurable processing stages.

The module implements the following design patterns:
- Abstract Base Classes for pipeline adapters
- Protocol-based processing stages
- Polymorphic data processing

Classes:
    ProcessingPipeline: Abstract base class for pipeline adapters.
    JSONAdapter: Pipeline adapter for dictionary data.
    CSVAdapter: Pipeline adapter for string data.
    StreamAdapter: Pipeline adapter for list data.
    ProcessingStage: Protocol defining the stage interface.
    InputSatge: Input validation and parsing stage.
    TransformStage: Data transformation and enrichment stage.
    OutputStage: Output formatting and delivery stage.
    OutputFormatStage: Additional output formatting stage.
    NexusManager: Manager for coordinating multiple pipelines.

    credit:
    all docstring made by IA
"""

from abc import ABC, abstractmethod
from typing import Protocol, Any, Union, List, Dict
import time


# =============================================================================
# ===================== Pipelines classes =====================================
# =============================================================================


class ProcessingPipeline(ABC):
    """Abstract base class for data processing pipelines.

    This class provides the foundation for different pipeline adapters
    that can process various data formats through multiple stages.
    """

    def __init__(self, pipeline_id: str) -> None:
        """Initialize the processing pipeline.

        Args:
            pipeline_id: Unique identifier for the pipeline.
        """
        self.pipeline_id = pipeline_id
        self.stages = []

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        """Process data through the pipeline stages.

        Args:
            data: Input data to be processed.

        Returns:
            Processed data or error message.
        """
        pass

    def add_stage(self, stages: List) -> None:
        """Add processing stages to the pipeline.

        Args:
            stages: List of processing stage objects to add.
        """
        for stage in stages:
            if (type(stage).__name__ in [type(stage).__name__
                                         for stage in self.stages]):
                print(f"{type(stage).__name__} already added, skipping")
            else:
                self.stages.append(stage)


class JSONAdapter(ProcessingPipeline):
    """Pipeline adapter for processing JSON dictionary data.

    This adapter processes dictionary-type data through configured stages.
    """

    def __init__(self, pipeline_id: str) -> None:
        """Initialize the JSON adapter.

        Args:
            pipeline_id: Unique identifier for the pipeline.
        """
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        """Process dictionary data through pipeline stages.

        Args:
            data: Input data containing dictionary elements.

        Returns:
            Processed data or None if an error occurs.
        """
        try:
            for d in data:
                if (isinstance(d, dict)):
                    data = d
                    print(f"Input: {data}")
                    if (len(self.stages) == 0):
                        raise Exception("Error JSONAdapter: no stages given")
                    for stage in self.stages:
                        data = stage.process(data)  # small pipeline exemple
                    return data

            raise Exception("Error JSONAdapter: no valide data type found")
        except (Exception, AttributeError) as e:
            print(e)
            return None


class CSVAdapter(ProcessingPipeline):
    """Pipeline adapter for processing CSV string data.

    This adapter processes string-type data through configured stages.
    """

    def __init__(self, pipeline_id: str) -> None:
        """Initialize the CSV adapter.

        Args:
            pipeline_id: Unique identifier for the pipeline.
        """
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        """Process string data through pipeline stages.

        Args:
            data: Input data containing string elements.

        Returns:
            Processed data or None if an error occurs.
        """
        try:
            for d in data:
                if (isinstance(d, str)):
                    data = d
                    print(f"Input: {data}")
                    if (len(self.stages) <= 0):
                        raise Exception("Error CSVAdapter: no stages given")
                    for stage in self.stages:
                        data = stage.process(data)  # small pipeline exemple
                    return data

            raise Exception("Error CSVAdapter: no valide data type found")
        except (Exception, AttributeError) as e:
            print(e)
            return None


class StreamAdapter(ProcessingPipeline):
    """Pipeline adapter for processing stream list data.

    This adapter processes list-type data through configured stages.
    """

    def __init__(self, pipeline_id: str) -> None:
        """Initialize the stream adapter.

        Args:
            pipeline_id: Unique identifier for the pipeline.
        """
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        """Process list data through pipeline stages.

        Args:
            data: Input data containing list elements.

        Returns:
            Processed data or None if an error occurs.
        """
        try:
            for d in data:
                if (isinstance(d, list)):
                    data = d
                    print(f"Input: {data}")
                    if (len(self.stages) <= 0):
                        raise Exception("Error StreamAdapter: no stages given")
                    for stage in self.stages:
                        data = stage.process(data)  # small pipeline exemple
                    return data

            raise Exception("Error StreamAdapter: no valide data type found")
        except (Exception, AttributeError) as e:
            print(e)
            return None


# =============================================================================
# ========================== Stages classes ===================================
# =============================================================================


class ProcessingStage(Protocol):
    """Protocol defining the interface for processing stages.

    All processing stages must implement the process method.
    """

    def process(self, data: Any) -> Any:
        """Process input data.

        Args:
            data: Input data to process.

        Returns:
            Processed data.
        """
        pass


class InputSatge():
    """Input validation and parsing stage.

    This stage validates and parses input data for dictionary, string,
    and list formats, ensuring data structure integrity.
    """

    def process(self, data: Any) -> Any:
        """Validate and parse input data.

        Args:
            data: Input data to validate (dict, str, or list).

        Returns:
            Validated data or None if validation fails.
        """
        if (isinstance(data, dict)):
            try:
                for key, value in data.items():
                    if (key not in ["sensor", "value", "unit"]):
                        raise Exception("Error S1: Invalide data structure,  \
valid structur is {sensor : ... , value : ... , unit : ...}")
                    if ((isinstance(value, int) or isinstance(value, float))
                       is False and isinstance(value, str) is False):
                        raise ValueError(f"Error S1: Invalide data found in \
data, {value} is not a string or an integer")
                data["value"] = float(data["value"])
            except (Exception, ValueError) as e:
                print(e)
                return None
            else:
                return data

        elif (isinstance(data, str)):
            try:
                data_split = data.split(",")
                if (len(data_split) < 3):
                    raise Exception("Error S1: Invalide data structure, \
valid structur is 'user,action,timestamp'")
            except (Exception) as e:
                print(e)
                return None
            else:
                return data

        elif (isinstance(data, list)):
            try:
                for temp in data:
                    float(temp)
            except (Exception) as e:
                print(e)
                return None
            else:
                return data
        print("Error S1: Invalide data type")


class TransformStage():
    """Data transformation and enrichment stage.

    This stage transforms and enriches validated data by adding
    type labels and structuring the data format.
    """

    def process(self, data: Any) -> Dict:
        """Transform and enrich data with type information.

        Args:
            data: Validated input data (dict, str, or list).

        Returns:
            Transformed data with type label or None if transformation fails.
        """
        if (isinstance(data, dict)):
            try:
                return ["Sensor_data", data]
            except Exception:
                print("Error S2: invalid data forma")
                return None

        if (isinstance(data, str)):
            try:
                return ["Log_data", {key: value for key, value
                                     in zip(["user", "action", "timestamp"],
                                            data.split(","))}]
            except Exception:
                print("Error S2: invalid data forma")
                return None

        if (isinstance(data, list)):
            try:
                return ["measure_data",
                        {"temp_" + str(i): data[i] for i in range(0, len(data))
                         }]
            except Exception as e:
                print(e)
                return None
        return data


class OutputStage():
    """Output formatting and delivery stage.

    This stage formats processed data into human-readable output
    strings based on data type.
    """

    def process(self, data: Any) -> str:
        """Format processed data into output strings.

        Args:
            data: Transformed data with type label.

        Returns:
            Formatted output string.
        """
        if (data is None):
            return "An Error occured, output can't be generated"

        if (data[0] == "Sensor_data"):
            data = data[1]
            if (data["sensor"] == "temp"):
                return f"Processed temperatur reading: \
{data['value']} {data['unit']} (Normal Range)"
            if (data["sensor"] == "hum"):
                return f"Processed humidity reading: \
{data['value']} {data['unit']} (Normal Range)"

        if (data[0] == "Log_data"):
            data = data[1]
            return f"User: {data['user']}, activity: \
{data['action']} at {data['timestamp']}, 1 actions processed"

        if (data[0] == "measure_data"):
            data = data[1]
            return f"Stream summary: \
{len(data)} readings, avg: {round(sum(data.values()) / len(data), 2)}°C"


class OutputFormatStage():
    """Additional output formatting stage.

    This stage applies additional formatting transformations to output strings.
    """

    def process(self, data: Any) -> str:
        """Apply uppercase formatting to output data.

        Args:
            data: Input string data to format.

        Returns:
            Formatted data in uppercase or None if input is None.
        """
        if (data is None):
            return data
        return data.upper()

# =============================================================================
# ====================== Polymorphic class exemple ============================
# =============================================================================


class NexusManager():
    """Manager for coordinating multiple processing pipelines.

    This class manages multiple pipeline adapters and orchestrates
    data processing across different formats.
    """

    def __init__(self):
        """Initialize the Nexus Manager with an empty pipeline list."""
        self.piplines = []

    def add_pipeline(self, pipelines: Any) -> None:
        """Add pipelines to the manager.

        Args:
            pipelines: List of pipeline objects to add to the manager.
        """
        for pipeline in pipelines:
            if (pipeline in self.piplines):
                print(f"{type(pipeline).__name__} already added, skipping")
            else:
                self.piplines.append(pipeline)

    def process_data(self, data: Any):
        """Process data through all registered pipelines.

        Args:
            data: Input data to process through all pipelines.
        """
        input = InputSatge()
        transform = TransformStage()
        output = OutputStage()
        # outputformat = OutputFormatStage()

        for pipeline in self.piplines:
            print(f"\nProcessing {pipeline.pipeline_id[:-3]} \
data through pipeline...")
            pipeline.add_stage([input, transform, output])
            result_str = pipeline.process(data)
            print("Tranform: Formating and Enriching data")
            print(f"Output: {result_str}")
        for pipeline in self.piplines:
            pipeline.stages = []


if (__name__ == "__main__"):
    data = [
        {"sensor": "temp", "value": 23.5, "unit": "°C"},
        [12, 78, 4.0, 15.0, -4, 2],
        "Shinji,login,8h30"
    ]

    data_corrupted = [
        {"%&@@#j*H": "temp", "value": 23.5, "unit": "°C"},
        [12, None, 4.0, 15.0, None, 2],
        "...."
    ]

    input = InputSatge()
    transform = TransformStage()
    output = OutputStage()
    outputformat = OutputFormatStage()

    JSON = JSONAdapter("JSON_01")
    CSV = CSVAdapter("CSV_01")
    STREAM = StreamAdapter("Stream_01")

    nexus = NexusManager()

    print(" CODE NEXUS - ENTREPRISE PIPELINE SYSTEM ".center(41 + 6, "="))

    print("\nInitializing Nexus Manager...")
    print("Pipeline capacity: 1000 stream/second\n")

    print("Creating Data Processing Pipeline...")

    print("Stage 1: Input validation and parsing")
    print("Stage 2: Data transformation and enrichment")
    print("Stage 3: Output formatting and delivery\n")

    print(" Multi-Format Data Processing ".center(30 + 6, "="))

    nexus.add_pipeline([JSON, CSV, STREAM])
    nexus.process_data(data)

    print()
    print(" Pipeline Chaining Demo ".center(24 + 6, "="))
    print("Pipeline A -> Pipeline B -> Pipeline C")
    print("Data flow: Raw -> Processed -> Analyzed -> Stored\n")

    start = time.time()
    chain_result = OutputStage().process(TransformStage().
                                         process(InputSatge().
                                                 process([4, 10, 8, 7,
                                                          2, 5, 4])))
    end = time.time()
    t = round(end - start, 7)

    print(f"Chain result: {chain_result}")
    print(f"Performance: {round((0.0000069 / t) * 100, 2)}% efficiency, \
{'{:.7f}'.format(t)}s total processing time\n")

    print(" Error Recovery Test ".center(21 + 6, "="))
    print("simulating pipeline failur...")
    nexus.process_data(data_corrupted)

    print("\nNexus Integration complete. All systems operational.")
