#      Copyright 2020. ThingsBoard
#
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#
#          http://www.apache.org/licenses/LICENSE-2.0
#
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.

from simplejson import dumps, loads
from thingsboard_gateway.connectors.rest.rest_converter import RESTConverter, log
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class JsonRESTUplinkConverter(RESTConverter):
    def __init__(self, config):
        super().__init__()
        self.__log = log
        self.__config = config

    def convert(self, config, data):
        if isinstance(data, (bytes, str)):
            data = loads(data)
        dict_result = {"deviceName": None, "deviceType": None, "attributes": [], "telemetry": []}
        try:
            if self.__config.get("deviceNameExpression") is not None:
                dict_result["deviceName"] = TBUtility.get_value(self.__config.get("deviceNameExpression"), data, expression_instead_none=True)
            else:
                log.error("The expression \"%s\" for looking \"deviceName\" not found in config %s", dumps(self.__config))
            if self.__config.get("deviceTypeExpression") is not None:
                dict_result["deviceType"] = TBUtility.get_value(self.__config.get("deviceTypeExpression"), data, expression_instead_none=True)
            else:
                log.error("The expression for looking \"deviceType\" not found in config %s", dumps(self.__config))
            dict_result["attributes"] = []
            dict_result["telemetry"] = []
        except Exception as e:
            log.exception(e)
        try:
            if self.__config.get("attributes"):
                for attribute in self.__config.get("attributes"):
                    attribute_value = TBUtility.get_value(attribute["value"], data, attribute["type"])
                    if attribute_value is not None:
                        dict_result["attributes"].append({attribute["key"]: attribute_value})
                    else:
                        log.debug("%s key not found in response: %s", attribute["value"].replace("${", '"').replace("}", '"'), data)
        except Exception as e:
            log.error('Error in the JSON Rest Uplink converter, using config: \n%s\n and message: \n%s\n', dumps(self.__config), data)
            log.exception(e)
        try:
            if self.__config.get("timeseries"):
                for timeseries in self.__config.get("timeseries"):
                    timeseries_value = TBUtility.get_value(timeseries["value"], data, timeseries["type"])
                    if timeseries_value is not None:
                        dict_result["telemetry"].append({timeseries["key"]: timeseries_value})
                    else:
                        log.debug("%s key not found in response: %s", timeseries["value"].replace("${", '"').replace("}", '"'), data)
        except Exception as e:
            log.exception(e)
        return dict_result
