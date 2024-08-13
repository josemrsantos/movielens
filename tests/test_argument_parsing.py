import pytest
from movielens_pipeline import parse_args
from unittest.mock import patch


def test_when_valid_args_then_correctly_parsed():
    test_args = ["--input-path", "/path/to/input", "--output-path", "/path/to/output"]
    with patch('sys.argv', ["movielens_pipeline.py"] + test_args):
        args = parse_args()
        assert args.input_path == "/path/to/input"
        assert args.output_path == "/path/to/output"


def test_when_missing_input_path_then_system_exit():
    test_args = ["--output-path", "/path/to/output"]
    with patch('sys.argv', ["movielens_pipeline.py"] + test_args):
        with pytest.raises(SystemExit):
            parse_args()


def test_when_missing_output_path_then_system_exit():
    test_args = ["--input-path", "/path/to/input"]
    with patch('sys.argv', ["movielens_pipeline.py"] + test_args):
        with pytest.raises(SystemExit):
            parse_args()
