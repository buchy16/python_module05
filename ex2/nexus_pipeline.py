from abc import ABC, abstractmethod
from typing import Protocol, Any, Union, List, Dict


# =============================================================================
# ===================== Pipelines classes =====================================
# =============================================================================


class ProcessingPipeline(ABC):
    def __init__(self, pipeline_id) -> None:
        self.pipeline_id = pipeline_id
        self.stages = []

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        pass

    def add_stage(self, satge: Any) -> None:
        pass


class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id) -> None:
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        pass


class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id) -> None:
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        pass


class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id) -> None:
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        pass


# =============================================================================
# ========================== Stages classes ===================================
# =============================================================================


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        pass


class InputSatge():
    def process(self, data: Any) -> Any:
        if (isinstance(data, dict)):
            try:
                for key, value in data.items():
                    if (key not in ["sensor", "value", "unit"]):
                        raise Exception("Error S1: Invalide data structure, valid structur is {sensor : ... , value : ... , unit : ...}")
                    if ((isinstance(value, int) or isinstance(value, float)) is False and isinstance(value, str) is False):
                        raise ValueError(f"Error S1: Invalide data found in data, {value} is not a string or an integer")
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
                    raise Exception("Error S1: Invalide data structure, valid structur is 'user,action,timestamp'")
            except (Exception) as e:
                print(e)
                return None
            else:
                return data

        elif (isinstance(data, list)):
            try:
                for temp in data:
                    float(temp)
            except (ValueError) as e:
                print(e)
                return None
            else:
                return data
        print("Error S1: Invalide data type")


class TransformStage():
    def process(self, data: Any) -> Dict:
        if (isinstance(data, dict)):
            try:
                return ["Sensor_data", data]
            except Exception:
                print("Error S2: invalid data forma")
                return None

        if (isinstance(data, str)):
            try:
                return ["Log_data", {key: value for key, value in zip(["user", "action", "timestamp"], data.split(","))}]
            except Exception:
                print("Error S2: invalid data forma")
                return None

        if (isinstance(data, list)):
            try:
                return ["measure_data", {"temp_" + str(i): data[i] for i in range(0, len(data))}]
            except Exception as e:
                print(e)
                return None
        return data


class OutputStage():
    def process(self, data: Any) -> str:
        if (data is None):
            return "An Error occured, output can't be generated"

        if (data[0] == "Sensor_data"):
            data = data[1]
            if (data["sensor"] == "temp"):
                return f"Processed temperatur reading: {data["value"]} {data["unit"]} (Normal Range)"
            if (data["sensor"] == "hum"):
                return f"Processed humidity reading: {data["value"]} {data["unit"]} (Normal Range)"

        if (data[0] == "Log_data"):
            data = data[1]
            return f"User: {data["user"]}, activity: {data["action"]} at {data["timestamp"]}, 1 actions processed"

        if (data[0] == "measure_data"):
            data = data[1]
            return f"Stream summary: {len(data)} readings, avg: {sum(data.values())}°C"




# =============================================================================
# ====================== Polymorphic class exemple ============================
# =============================================================================


class NexusManager():
    def __init__(self):
        self.piplines = []

    def add_pipeline(self, stage: Any) -> None:
        pass

    def process_data(self, data: Any):
        pass






if (__name__ == "__main__"):
    print(" CODE NEXUS - ENTREPRISE PIPELINE SYSTEM ".center(41 + 6, "="))

    print("\nInitializing Nexus Manager...")
    print("Pipeline capacity: 1000 stream/second")

    print("Creating Data Processing Pipeline...")
    input = InputSatge()
    transform = TransformStage()
    output = OutputStage()
    print("Stage 1: Input validation and parsing")
    print("Stage 2: Data transformation and enrichment")
    print("Stage 3: Output formatting and delivery\n")

    print(" Multi-Format Data Processing ".center(30 + 6, "="))
    print("\nProcessing JSON data through pipeline...")


    print(output.process(transform.process(input.process("Shinji,login,8h30"))))
    print(output.process(transform.process(input.process("Shinji,login"))))

    print(output.process(transform.process(input.process({"sensor": "temp", "value": 23.5, "unit": "°C"}))))
    print(output.process(transform.process(input.process({"car": "temp", "value": 23.5, "unit": "°C"}))))

    print(output.process(transform.process(input.process([12, 78, 4.0, 15.0, -4, 2]))))
    print(output.process(transform.process(input.process([12, 78, 4.0, "Emilie", -4, 2]))))

