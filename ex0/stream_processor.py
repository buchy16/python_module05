from abc import ABC, abstractmethod
from typing import Any, List


class DataProcessor(ABC):
    """A general abstracted class for data manipulation with 3 methods
    process(), validate(), format_output()

    Args:
        ABC (class): ABC class for abstracted class
    """
    @abstractmethod
    def process(self, data: Any) -> str:
        """A methode that will process the data and return a string
        with data info

        Args:
            data (Any): data to manipulate

        Returns:
            str: A string with infos about the data
        """
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """A methode that will check if the data is valide or not

        Args:
            data (Any): data to validate

        Returns:
            bool: True if the data is validated, false if it's not
        """
        pass

    def format_output(self, result: str) -> str:
        """A simple methode that will format the given string (str)
        and add "Output: " at the begining

        Args:
            result (str): string to modify

        Returns:
            str: the original string but mofied
        """
        return ("Output: " + result)


class NumericProcessor(DataProcessor):
    """A class that inherit from DataProcessor, the data manipulated will be
    a list of integers.
    2 overrided methods: process(), validated()
    1 method inherited: format_output()

    Args:
        DataProcessor (class): the abstracted class for data manipulation
    """
    def process(self, data: List[int]) -> str:
        """A methode that will process the data and return a string
        with data info

        Args:
            data (List[int]): List of integers

        Returns:
            str: A string with infos about the data
        """
        if (self.validate(data) is True):
            return (f"Processed {len(data)} numeric values, \
sum={sum(data)}, vg={sum(data) / len(data)}")
        else:
            return "Error: data was not validate, please verify your input"

    def validate(self, data: List[int]) -> bool:
        """A methode that will check if the data is valide or not

        Args:
            data (List[int]): List of integers

        Returns:
            bool: True if the data is validated, false if it's not
        """
        try:
            if (isinstance(data, list) is False):
                raise Exception(f"Error data is not a list, data type -> \
{type(data)}")
            if (len(data) == 0):
                raise Exception("Error data is emtpy")
            for number in data:
                int(number)
        except (Exception, ValueError) as e:
            print(e)
            return (False)
        else:
            return (True)

    def format_output(self, result: str) -> str:
        return super().format_output(result)


class TextProcessor(DataProcessor):
    """A class that inherit from DataProcessor, the data manipulated will be
    a string.
    2 overrided methods: process(), validated()
    1 method inherited: format_output()

    Args:
        DataProcessor (class): the abstracted class for data manipulation
    """
    def process(self, data: str) -> str:
        """A methode that will process the data string and return a string
        with data info

        Args:
            data (str): string to manipulate

        Returns:
            str: A string with infos about the data string
        """
        if (self.validate(data) is True):
            return (f"processed text: {len(data)} \
characters, {len(data.split(' '))} words")
        else:
            return "Error: data was not validate, please verify your input"

    def validate(self, data: str) -> str:
        """A methode that will check if the data string is valide or not

        Args:
            data (str): string to validate

        Returns:
            bool: True if the data string is validated, false if it's not
        """
        try:
            if (isinstance(data, str) is False):
                raise Exception(f"Error data is not a str, data type -> \
{type(data)}")
            if (len(data) == 0):
                raise Exception("Error data is empty")
        except Exception as e:
            print(e)
            return (False)
        else:
            return (True)

    def format_output(self, result: str) -> str:
        return super().format_output(result)


class LogProcessor(DataProcessor):
    """A class that inherit from DataProcessor, the data manipulated will be
    a string with a specific format.
    2 overrided methods: process(), validated()
    1 method inherited: format_output()

    Args:
        DataProcessor (class): the abstracted class for data manipulation
    """
    def process(self, data: str) -> str:
        """A methode that will process the data log string and return a string
        with data info

        Args:
            data (str): log string to manipulate

        Returns:
            str: A string with infos about the data log string
        """
        if (self.validate(data) is True):
            log = data.split(":")
            if (log[0] == "ERROR"):
                return (f"[ALERT] {log[0]} level detected:{log[1]}")
            if (log[0] == "INFO"):
                return (f"[INFO] {log[0]} level detected:{log[1]}")
        return "Error: data was not validate, please verify your input"

    def validate(self, data: str) -> bool:
        """A methode that will check if the data log string is valide or not

        Args:
            data (str): log string to validate

        Returns:
            bool: True if the data log string is validated, false if it's not
        """
        try:
            if (isinstance(data, str) is False):
                raise Exception(f"Error data is not a log_str, data type -> \
{type(data)}")
            if (len(data) == 0):
                raise Exception("Error data is empty")
            if (len(data.split(":")) == 0):
                raise Exception("Error data is not a log_str")
        except (Exception) as e:
            print(e)
            return False
        else:
            return (True)

    def format_output(self, result: str) -> str:
        return super().format_output(result)


if (__name__ == "__main__"):
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    print("Initializing Numeric Processor...")
    data = [1, 2, 3, 4, 5]
    print(f"Processing data: {data}")
    string = NumericProcessor().process(data)
    if (NumericProcessor().validate(data) is True):
        print("Validation: Numeric data verified")
    print(NumericProcessor().format_output(string))

    print("\nInitializing Text Processor...")
    data = "Hello Nexus World"
    print(f"Processing data: '{data}'")
    string = TextProcessor().process(data)
    if (TextProcessor().validate(data) is True):
        print("Validation: Text data verified")
    print(TextProcessor().format_output(string))

    print("\nInitializing Log Processor...")
    data = "ERROR: Connection timeout"
    print(f"Processing data: '{data}'")
    string = LogProcessor().process(data)
    if (LogProcessor().validate(data) is True):
        print("Validation: Log entry verified")
    print(LogProcessor().format_output(string))

    print("\n=== Polymorphic Processing Demo ===\n")
    print("Processing multiple data types through same interface...")
    types = [NumericProcessor(), TextProcessor(), LogProcessor()]
    datas = [[2, 2, 2], "Hello World", "INFO: System ready"]
    i = 1

    for type, data in zip(types, datas):
        print(f"Result {i}: {type.process(data)}")
        i += 1

    print("\nFoundation systems online. Nexus redy for advanced streams.")
