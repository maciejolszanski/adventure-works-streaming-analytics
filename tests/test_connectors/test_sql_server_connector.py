import pytest
from unittest.mock import MagicMock, patch
import pyodbc
from streaming_analytics.connectors.sql_server_connector import SQLServerConnector

# Mock the logger to avoid actual logging output during tests
@pytest.fixture
def mock_logger():
    with patch("streaming_analytics.connectors.sql_server_connector.logger") as mock:
        yield mock

# Mock the pyodbc.connect to avoid actual DB connection
@pytest.fixture
def mock_connect():
    with patch("pyodbc.connect") as mock:
        yield mock


def test_initialization_with_connection_string(mock_logger, mock_connect):
    mock_connect.return_value = MagicMock()
    connector = SQLServerConnector(connection_string="mock_connection_string")
    mock_connect.assert_called_once_with("mock_connection_string")
    assert connector.connection is not None


def test_initialization_without_connection_string(mock_logger, mock_connect):
    with patch("streaming_analytics.connectors.sql_server_connector.SQLServerConnector._get_conn_str_from_env_file", return_value="mock_connection_string"):
        mock_connect.return_value = MagicMock()
        connector = SQLServerConnector()
        mock_connect.assert_called_once_with("mock_connection_string")
        assert connector.connection is not None


def test_get_conn_str_from_env_file(mock_logger, mock_connect):
    with patch("os.getenv") as mock_getenv:
        mock_getenv.side_effect = lambda key: {
            'SQL_SERVER_HOST': 'localhost',
            'SQL_SERVER_DATABASE': 'test_db',
            'SQL_SERVER_USER': 'user',
            'SQL_SERVER_PASSWORD': 'password'
        }.get(key)
        
        connector = SQLServerConnector()
        connection_string = connector._get_conn_str_from_env_file()
        expected_connection_string = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=localhost;"
            "DATABASE=test_db;"
            "UID=user;"
            "PWD=password"
        )
        assert connection_string == expected_connection_string


def test_connect(mock_logger, mock_connect):
    # Mock pyodbc.connect
    mock_connect.return_value = MagicMock()
    connector = SQLServerConnector(connection_string="mock_connection_string")
    mock_connect.assert_called_once_with("mock_connection_string")
    assert connector.connection is not None


def test_connect_failure(mock_logger, mock_connect):
    # Simulate connection failure by making pyodbc.connect raise an exception
    mock_connect.side_effect = pyodbc.Error
    with pytest.raises(ConnectionError):
        SQLServerConnector(connection_string="mock_connection_string")


def test_close_connection(mock_logger, mock_connect):
    # Mock the connection and the close method
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection  # Ensure mock connection is used
    connector = SQLServerConnector(connection_string="mock_connection_string")
    connector.connection = mock_connection  # Set connection to mock
    connector.close_connection()
    mock_connection.close.assert_called_once()


def test_close_connection_error(mock_logger, mock_connect):
    # Mock the connection and simulate an error during close
    mock_connection = MagicMock()
    mock_connection.close.side_effect = Exception("Close error")
    mock_connect.return_value = mock_connection
    connector = SQLServerConnector(connection_string="mock_connection_string")
    connector.connection = mock_connection  # Set connection to mock
    with patch.object(mock_logger, "error") as mock_error:
        connector.close_connection()
        mock_error.assert_called_once_with("Couldn't close the connection due to an error: Close error")


def test_execute_select_query(mock_logger, mock_connect):
    # Mock the cursor and description for a SELECT query
    mock_cursor = MagicMock()
    mock_cursor.description = [("col1",), ("col2",)]
    mock_cursor.fetchall.return_value = [(1, 2), (3, 4)]
    
    # Mock the connection and cursor execution
    mock_connection = MagicMock(cursor=MagicMock(return_value=mock_cursor))
    mock_connect.return_value = mock_connection
    connector = SQLServerConnector(connection_string="mock_connection_string")
    column_names, rows = connector.execute_query("SELECT * FROM table")
    
    assert column_names == ["col1", "col2"]
    assert rows == [(1, 2), (3, 4)]


def test_execute_non_select_query(mock_logger, mock_connect):
    # Mock the commit method for non-SELECT queries
    mock_connection = MagicMock(commit=MagicMock())
    mock_connect.return_value = mock_connection
    connector = SQLServerConnector(connection_string="mock_connection_string")
    column_names, rows = connector.execute_query("INSERT INTO table (col1) VALUES (1)")
    
    assert column_names == []
    assert rows == []


def test_execute_query_error(mock_logger, mock_connect):
    # Mock an exception during query execution
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = Exception("Query execution error")
    mock_connection = MagicMock(cursor=MagicMock(return_value=mock_cursor))
    mock_connect.return_value = mock_connection
    
    connector = SQLServerConnector(connection_string="mock_connection_string")
    with patch.object(mock_logger, "error") as mock_error:
        with pytest.raises(Exception):
            connector.execute_query("SELECT * FROM non_existing_table")

        mock_error.assert_called_once()
