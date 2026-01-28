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
    def process(self, data: Any) -> Dict:
        pass
    # vraie implémentation de la méthode


class TransformStage():
    def process(self, data: Any) -> Dict:
        pass
    # vraie implémentation de la méthode


class OutputStage():
    def process(data) -> str:
        pass
    # vraie implémentation de la méthode


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
    print(ProcessingPipeline.stages)
    print(JSONAdapter.stages)
