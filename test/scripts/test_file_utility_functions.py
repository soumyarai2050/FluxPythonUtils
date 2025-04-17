import pytest
import tempfile
from pathlib import PurePath
import polars as pl
from typing import List, Optional, Dict

from FluxPythonUtils.scripts.file_utility_functions import dict_or_list_records_polars_csv_reader
from FluxPythonUtils.scripts.model_base_utils import MsgspecBaseModel


class SampleRecord(MsgspecBaseModel, kw_only=True):
    name: str
    age: int
    email: Optional[str] = None
    is_active: bool = True


def test_dict_or_list_records_polars_csv_reader():
    # Create a temporary CSV file
    with tempfile.TemporaryDirectory() as temp_dir:
        # Define the path
        temp_dir_path = PurePath(temp_dir)
        csv_filename = "test_records.csv"
        csv_path = temp_dir_path / csv_filename

        # Create test data
        test_data = [
            {"name": "John Doe", "age": 30, "email": "john@example.com", "is_active": True},
            {"name": "Jane Smith", "age": 25, "email": "", "is_active": False},
            {"name": "Bob Johnson", "age": 40, "email": "bob@example.com", "is_active": True},
        ]

        # Write test data to CSV
        df = pl.DataFrame(test_data)
        df.write_csv(str(csv_path))

        # Call the function being tested
        result: List[SampleRecord] = dict_or_list_records_polars_csv_reader(
            SampleRecord,
            csv_path,
            rename_col_names_to_snake_case=False,
            rename_col_names_to_lower_case=True
        )

        # Assertions
        assert len(result) == 3

        # Check first record
        assert result[0].name == "John Doe"
        assert result[0].age == 30
        assert result[0].email == "john@example.com"
        assert result[0].is_active is True

        # Check second record - empty string should be None
        assert result[1].name == "Jane Smith"
        assert result[1].age == 25
        assert result[1].email is None  # This was an empty string in the CSV
        assert result[1].is_active is False

        # Check third record
        assert result[2].name == "Bob Johnson"
        assert result[2].age == 40
        assert result[2].email == "bob@example.com"
        assert result[2].is_active is True


def test_dict_or_list_records_polars_csv_reader_with_rename():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = PurePath(temp_dir)
        csv_filename = "test_camel_case.csv"
        csv_path = temp_dir_path / csv_filename

        # Changed camelCase column names to match the expected field names after conversion
        test_data = [
            {"Name": "John Doe", "Age": 30, "Email": "john@example.com", "isActive": True},
            {"Name": "Jane Smith", "Age": 25, "Email": "", "isActive": False},
        ]

        df = pl.DataFrame(test_data)
        df.write_csv(str(csv_path))

        result: List[SampleRecord] = dict_or_list_records_polars_csv_reader(
            SampleRecord,
            csv_path,
            rename_col_names_to_snake_case=True,
            rename_col_names_to_lower_case=True
        )

        assert len(result) == 2
        assert result[0].name == "John Doe"  # Name -> name
        assert result[0].age == 30  # Age -> age
        assert result[0].email == "john@example.com"  # Email -> email
        assert result[0].is_active is True  # isActive -> is_active


def test_dict_or_list_records_polars_csv_reader_file_not_found():
    # Test with a non-existent file
    with pytest.raises(Exception):
        dict_or_list_records_polars_csv_reader(
            SampleRecord,
            PurePath("/nonexistent/path.csv")
        )


class MixedNumericRecord(MsgspecBaseModel, kw_only=True):
    name: str
    age: int
    roll_no: str
    email: Optional[str] = None
    is_active: bool = True


def test_dict_or_list_records_polars_csv_reader_with_dtype_override():
    """
    Tests reading a CSV where a column has mixed integer/float strings,
    requiring a dtype override to parse correctly.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = PurePath(temp_dir)
        csv_filename = "test_mixed_numeric.csv"
        csv_path = temp_dir_path / csv_filename

        # CSV content simulating the original problem
        # 'roll_no' starts with integers, then has a float
        csv_content = """name,age,roll_no,email,is_active
John Doe,30,1,john@example.com,true
Jane Smith,25,2,,false
Bob Johnson,40,3.3,bob@example.com,true
Another One,55,5,another@example.com,true
"""
        # Write the raw string content to the file
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write(csv_content)

        # Define the necessary dtype override for the 'roll_no' column
        dtype_override: Dict[str, pl.DataType] = {"roll_no": pl.String}

        # Call the function with the dtype override
        result: List[MixedNumericRecord] = dict_or_list_records_polars_csv_reader(
            MixedNumericRecord, # Use the model with roll_no as float
            csv_path,
            dtype=dtype_override # Pass the override
        )

        # Assertions
        assert len(result) == 4

        # Check record types and values, especially roll_no
        assert isinstance(result[0], MixedNumericRecord)
        assert result[0].name == "John Doe"
        assert result[0].age == 30
        assert result[0].roll_no == "1" # Parsed as str
        assert result[0].email == "john@example.com"
        assert result[0].is_active is True

        assert isinstance(result[1], MixedNumericRecord)
        assert result[1].name == "Jane Smith"
        assert result[1].age == 25
        assert result[1].roll_no == "2" # Parsed as string
        assert result[1].email is None # Empty string converted to None
        assert result[1].is_active is False

        assert isinstance(result[2], MixedNumericRecord)
        assert result[2].name == "Bob Johnson"
        assert result[2].age == 40
        assert result[2].roll_no == "3.3" # Parsed correctly as string
        assert result[2].email == "bob@example.com"
        assert result[2].is_active is True

        assert isinstance(result[3], MixedNumericRecord)
        assert result[3].name == "Another One"
        assert result[3].age == 55
        assert result[3].roll_no == "5" # Parsed as string
        assert result[3].email == "another@example.com"
        assert result[3].is_active is True
