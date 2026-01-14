import pytest
import requests
from unittest.mock import patch
from src.main import receive_opa_account_num_from_address

@patch("requests.get")
def test_receive_valid_opa_account_num(mock_get):
    # Mock setup
    mock_opa_account_num = "883309050"
    mock_response = mock_get.return_value
    mock_response.status_code = 200
    mock_response.json.return_value = {"search_type": "address",
                                       "features": [
                                           {
                                               "properties": {
                                                   "opa_account_num": mock_opa_account_num,
                                               }
                                           }
                                       ]}
    
    result = receive_opa_account_num_from_address("1234 Market St")
    assert result == mock_opa_account_num
    