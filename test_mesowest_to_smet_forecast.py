import unittest
from unittest.mock import patch, mock_open
import requests
import json
from datetime import datetime
from mesowest_to_smet_forecast import mesowest_to_smet

class TestMesowestToSmet(unittest.TestCase):

    @patch('mesowest_to_smet_forecast.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_mesowest_to_smet(self, mock_file, mock_get):
        # Mock response from Mesowest API
        mock_response = {
            "STATION": [
                {
                    "STID": "TEST",
                    "NAME": "Test Station",
                    "LATITUDE": 40.0,
                    "LONGITUDE": -111.0,
                    "ELEV_DEM": 5000,
                    "OBSERVATIONS": {
                        "date_time": ["2024-07-20T00:00:00Z", "2024-07-20T01:00:00Z"],
                        "air_temp_set_1": [20.0, 21.0],
                        "surface_temp_set_1": [15.0, 16.0],
                        "relative_humidity_set_1": [50.0, 55.0],
                        "wind_speed_set_1": [5.0, 6.0],
                        "wind_direction_set_1": [180, 190],
                        "snow_depth_set_1": [100.0, 110.0],
                        "solar_radiation_set_1": [200.0, 210.0]
                    }
                }
            ]
        }
        mock_get.return_value.json.return_value = mock_response

        # Call the function
        start_time = "20240720000000"
        current_time = "20240720010000"
        stid = "TEST"
        make_input_plot = False
        forecast_bool = False

        mesowest_to_smet(start_time, current_time, stid, make_input_plot, forecast_bool)

        # Check if the correct API URL was called
        mock_get.assert_called_once_with(
            f'http://api.mesowest.net/v2/stations/timeseries?stid={stid}&token=3d5845d69f0e47aca3f810de0bb6fd3f&start={start_time}&end={current_time}'
        )

        # Check if the SMET file was written correctly
        mock_file.assert_called_once_with('TEST.smet', 'w')
        handle = mock_file()
        handle.write.assert_any_call('SMET 1.1 ASCII\n')
        handle.write.assert_any_call('[HEADER]\n')
        handle.write.assert_any_call('station_id       = TEST\n')
        handle.write.assert_any_call('station_name     = Test Station\n')
        handle.write.assert_any_call('latitude         = 40.0\n')
        handle.write.assert_any_call('longitude        = -111.0\n')
        handle.write.assert_any_call('altitude         = 1524.0\n')  # Elevation converted to meters
        handle.write.assert_any_call('nodata           = -999\n')
        handle.write.assert_any_call('tz               = 1\n')
        handle.write.assert_any_call('source           = University of Utah EFD Lab\n')
        handle.write.assert_any_call('fields           = timestamp TA RH TSG TSS HS VW DW ISWR\n')
        handle.write.assert_any_call('[DATA]\n')
        handle.write.assert_any_call('2024-07-20T00:00:00 293.15 0.50 273.15 288.15 0.10 5.00 180.00 200.00\n')
        handle.write.assert_any_call('2024-07-20T01:00:00 294.15 0.55 273.15 289.15 0.11 6.00 190.00 210.00\n')

if __name__ == '__main__':
    unittest.main()