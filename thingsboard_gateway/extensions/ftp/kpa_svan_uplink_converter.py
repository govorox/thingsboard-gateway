#     Copyright 2024. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import struct
import re
import time
from datetime import datetime

from thingsboard_gateway.connectors.ftp.ftp_uplink_converter import FTPUplinkConverter
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

class SvanTelemetry:
    def __init__(self):
        self.serial = ''
        self.device = ''
        self.function = ''
        self.start_time = ''
        self.updated = ''
        self.duration = ''
        pass

class KPASvanUplinkConverter(FTPUplinkConverter):
    def convert(self, config, data):
        try:
            delim = ';'
            
            if not data.startswith('UNIT TYPE:'):
                return {}
                
            lines = data.split('\n')
            if len(lines) < 8:
                return {}
            
            self._log.debug("data: %s", str(data))
            
            svan = SvanTelemetry()
            
            attributes = []
            telemetry = []
            last = ''
            timestamp = 0
            values = {}
            
            dict_result = {"deviceName": None, "deviceType": None, "attributes": [], "telemetry": []}
            
            for (index, line) in enumerate(lines):
                cols = line.split(delim)
                head = cols[0].strip()
                
                if head == '' and len(cols) > 1:
                    head = last
                    
                if head == 'UNIT TYPE:':
                    svan.device = cols[1].strip()
                    dict_result["deviceType"] = 'svan'
                    attributes.append({"model": svan.device})
                elif head == 'S/N:':
                    svan.serial = cols[1].strip()
                    dict_result["deviceName"] = 'svan-test-'+svan.serial
                    attributes.append({"serial": svan.serial})
                elif head == 'DEVICE FUNCTION:':
                    svan.function = cols[1].strip()
                    attributes.append({"function": svan.function})
                elif head == 'LAST UPDATED:':
                    svan.updated = cols[1] #;06/08/2024 12:20:00
                    attributes.append({"updated": svan.updated})
                elif head == 'MAIN RESULTS':
                    pass
                elif head == 'Start time:':
                    svan.start_time = cols[1].strip() #;06/08/2024 12:13:27
                    dt = datetime.strptime(svan.start_time, '%d/%m/%Y %H:%M:%S')
                    timestamp = int(datetime.timestamp(dt))*1000
                elif head == 'Measurement time:':
                    svan.duration = cols[1] # ;00:05:00
                elif head.startswith('Channel '):
                    channel = int(head.split(' ')[-1])
                    self._log.debug("channel: %d", channel)
                    if channel in [1,2,3] and len(cols) >= 3:
                        key = cols[1].strip().lower() + '_ch'+str(channel)
                        val = float(cols[2])
                        #telemetry.append({"ts": timestamp, "values": {key: val}})
                        values[key] = val
                
                last = head
                
            telemetry.append({"ts": timestamp, "values": values})

            dict_result["attributes"] = attributes
            dict_result["telemetry"] = telemetry
            
            self._log.debug("svan: %s. time: %s. result:%s", svan.serial, svan.start_time, str(dict_result))
            
            #self._log.debug("config: %s", str(config))
            #self._log.debug("data: %s", str(data))
            
            return dict_result

        except Exception as e:
            self._log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), body)
            self._log.exception(e)
