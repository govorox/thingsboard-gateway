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

import asyncio
import re
from queue import Queue
from random import choice
from string import ascii_lowercase
from threading import Thread
from time import sleep, monotonic

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.connectors.opcua.backward_compatibility_adapter import BackwardCompatibilityAdapter
from thingsboard_gateway.connectors.opcua.device import Device
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_logger import init_logger
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

try:
    import asyncua
except (ImportError, ModuleNotFoundError):
    print("OPC-UA library not found")
    TBUtility.install_package("asyncua")
    import asyncua

from asyncua.crypto.security_policies import SecurityPolicyBasic256Sha256, SecurityPolicyBasic256, \
    SecurityPolicyBasic128Rsa15
from asyncua.ua.uaerrors import UaStatusCodeError, BadNodeIdUnknown, BadConnectionClosed, \
    BadInvalidState, BadSessionClosed, BadAttributeIdInvalid, BadCommunicationError, BadOutOfService, BadNoMatch, \
    BadUnexpectedError, UaStatusCodeErrors, BadWaitingForInitialData

DEFAULT_UPLINK_CONVERTER = 'OpcUaUplinkConverter'

SECURITY_POLICIES = {
    "Basic128Rsa15": SecurityPolicyBasic128Rsa15,
    "Basic256": SecurityPolicyBasic256,
    "Basic256Sha256": SecurityPolicyBasic256Sha256,
}

MESSAGE_SECURITY_MODES = {
    "None": asyncua.ua.MessageSecurityMode.None_,
    "Sign": asyncua.ua.MessageSecurityMode.Sign,
    "SignAndEncrypt": asyncua.ua.MessageSecurityMode.SignAndEncrypt
}


class OpcUaConnector(Connector, Thread):
    def __init__(self, gateway, config, connector_type):
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        super().__init__()
        self._connector_type = connector_type
        self.__gateway = gateway
        self.__config = config
        self.__id = self.__config.get('id')
        self.name = self.__config.get("name", 'OPC-UA Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5)))

        # check if config is in old format and convert it to new format
        using_old_configuration_format = len(tuple(filter(lambda node_config: not node_config.get('deviceInfo'),
                                                          self.__config.get('server', {}).get('mapping', []))))
        self.__log = init_logger(self.__gateway, self.name, self.__config.get('logLevel', 'INFO'),
                                 enable_remote_logging=self.__config.get('enableRemoteLogging', False))
        if using_old_configuration_format:
            backward_compatibility_adapter = BackwardCompatibilityAdapter(self.__config, self.__log)
            self.__config = backward_compatibility_adapter.convert()

        self.__server_conf = self.__config.get('server', {})

        if using_old_configuration_format:
            self.__log.warning('Connector configuration has been updated to the new format.')

        if "opc.tcp" not in self.__server_conf.get("url"):
            self.__opcua_url = "opc.tcp://" + self.__server_conf.get("url")
        else:
            self.__opcua_url = self.__server_conf.get("url")

        self.__enable_subscriptions = self.__server_conf.get('enableSubscriptions', True)

        self.__data_to_send = Queue(-1)
        self.__sub_data_to_convert = Queue(-1)

        try:
            self.__loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.__loop)
        except RuntimeError:
            self.__loop = asyncio.get_event_loop()

        self.__client = None
        self.__subscription = None

        self.__connected = False
        self.__stopped = False
        self.daemon = True

        self.__device_nodes = []
        self.__next_poll = 0
        self.__next_scan = 0

    def open(self):
        self.__stopped = False
        self.start()
        self.__log.info("Starting OPC-UA Connector (Async IO)")

    def get_type(self):
        return self._connector_type

    def close(self):
        self.__stopped = True
        self.__connected = False
        self.__log.info("Stopping OPC-UA Connector")

        asyncio.run_coroutine_threadsafe(self.__disconnect(), self.__loop)
        asyncio.run_coroutine_threadsafe(self.__cancel_all_tasks(), self.__loop)

        start_time = monotonic()

        while self.is_alive():
            if monotonic() - start_time > 5:
                self.__log.error("Failed to stop connector %s", self.get_name())
                break
            sleep(.1)

        self.__log.info('%s has been stopped.', self.get_name())
        self.__log.stop()

    async def __cancel_all_tasks(self):
        for task in asyncio.all_tasks(self.__loop):
            task.cancel()

    async def __disconnect(self):
        try:
            await self.__client.disconnect()
            self.__log.info('%s has been disconnected from OPC-UA Server.', self.get_name())
        except Exception as e:
            self.__log.warning('%s could not be disconnected from OPC-UA Server: %s', self.name, e)

    async def __reset_node(self, node):
        node['valid'] = False
        if node.get('sub_on', False):
            try:
                if self.__subscription:
                    await self.__subscription.unsubscribe(node['subscription'])
            except Exception as e:
                self.__log.exception('Error unsubscribing on data change: %s', e)
            node['subscription'] = None
            node['sub_on'] = False

    async def __reset_nodes(self, device_name=None):
        for device in self.__device_nodes:
            if device_name is None or device.name == device_name:
                for section in ('attributes', 'timeseries'):
                    for node in device.values.get(section, []):
                        await self.__reset_node(node)

        if device_name is None and self.__subscription:
            try:
                await self.__subscription.delete()
            except Exception as e:
                self.__log.exception('Error deleting subscription: %s', e)
            self.__subscription = None

    def get_id(self):
        return self.__id

    def get_name(self):
        return self.name

    def is_connected(self):
        return self.__connected

    def is_stopped(self):
        return self.__stopped

    def get_config(self):
        return self.__config

    def run(self):
        data_send_thread = Thread(name='Send Data Thread', target=self.__send_data, daemon=True)
        data_send_thread.start()

        if self.__enable_subscriptions:
            sub_data_convert_thread = Thread(name='Sub Data Convert Thread', target=self.__convert_sub_data,
                                             daemon=True)
            sub_data_convert_thread.start()

        try:
            self.__loop.run_until_complete(self.start_client())
        except asyncio.CancelledError:
            self.__loop.run_until_complete(self.__client.disconnect())
            pass
        except Exception as e:
            self.__log.exception("Error in main loop: %s", e)

    async def start_client(self):
        while not self.__stopped:
            try:
                self.__client = asyncua.Client(url=self.__opcua_url,
                                               timeout=self.__server_conf.get('timeoutInMillis', 4000) / 1000)
                self.__client.session_timeout = self.__server_conf.get('sessionTimeoutInMillis', 120000)
                if self.__server_conf["identity"].get("type") == "cert.PEM":
                    await self.__set_auth_settings_by_cert()
                if self.__server_conf["identity"].get("username"):
                    self.__set_auth_settings_by_username()

                if self.__stopped:
                    break

                await self.retry_with_backoff(self.__client.connect)

                if not self.__client.uaclient.protocol:
                    self.__log.error("Failed to connect to server, retrying...")
                    await self.disconnect_if_connected()
                    continue
                self.__connected = True

                try:
                    await self.__client.load_data_type_definitions()
                except Exception as e:
                    self.__log.error("Error on loading type definitions:\n %s", e)

                poll_period = int(self.__server_conf.get('pollPeriodInMillis', 5000) / 1000)
                scan_period = int(self.__server_conf.get('scanPeriodInMillis', 3600000) / 1000)

                while not self.__stopped:
                    if monotonic() >= self.__next_scan:
                        self.__next_scan = monotonic() + scan_period
                        await self.__scan_device_nodes()

                    if monotonic() >= self.__next_poll:
                        self.__next_poll = monotonic() + poll_period
                        await self.__poll_nodes()

                    current_time = monotonic()
                    time_to_sleep = min(self.__next_poll - current_time, self.__next_scan - current_time)
                    if time_to_sleep > 0:
                        await asyncio.sleep(time_to_sleep)

            except (ConnectionError, BadSessionClosed):
                self.__log.warning('Connection lost for %s', self.get_name())
            except asyncio.exceptions.TimeoutError:
                self.__log.warning('Failed to connect %s', self.get_name())
                await self.disconnect_if_connected()
                await asyncio.sleep(5)
            except asyncio.CancelledError as e:
                if self.__stopped:
                    self.__log.debug('Task was cancelled due to connector stop: %s', e.__str__())
                else:
                    self.__log.exception('Task was cancelled: %s', e.__str__())
            except UaStatusCodeError as e:
                self.__log.error('Error in main loop, trying to reconnect: %s', exc_info=e)
                await self.disconnect_if_connected()
            except Exception as e:
                self.__log.exception("Error in main loop: %s", e)
            finally:
                if self.__stopped:
                    self.__connected = False
                await asyncio.sleep(.5)

    async def disconnect_if_connected(self):
        if self.__connected:
            try:
                await self.__client.disconnect()
            except TimeoutError:
                pass  # ignore

    async def retry_with_backoff(self, func, *args, max_retries=8, initial_delay=1, backoff_factor=2):
        delay = initial_delay
        for attempt in range(max_retries):
            try:
                return await func(*args)
            except Exception as e:
                self.__log.error('Encountered error: %r. Retrying in %s seconds...', e, delay)
                await asyncio.sleep(delay)
                delay *= backoff_factor
        self.__log.error('Max retries reached. Could not connect due to too many sessions.')
        return None

    async def __set_auth_settings_by_cert(self):
        try:
            ca_cert = self.__server_conf["identity"].get("caCert")
            private_key = self.__server_conf["identity"].get("privateKey")
            cert = self.__server_conf["identity"].get("cert")
            policy = self.__server_conf["security"]
            mode = self.__server_conf["identity"].get("mode", "SignAndEncrypt")

            if cert is None or private_key is None:
                self.__log.exception("Error in ssl configuration - cert or privateKey parameter not found")
                raise RuntimeError("Error in ssl configuration - cert or privateKey parameter not found")

            await self.__client.set_security(
                SECURITY_POLICIES[policy],
                certificate=cert,
                private_key=private_key,
                server_certificate=ca_cert,
                mode=MESSAGE_SECURITY_MODES[mode]
            )
        except Exception as e:
            self.__log.exception(e)

    def __set_auth_settings_by_username(self):
        self.__client.set_user(self.__server_conf["identity"].get("username"))
        if self.__server_conf["identity"].get("password"):
            self.__client.set_password(self.__server_conf["identity"].get("password"))

    def __load_converter(self, device):
        converter_class_name = device.get('converter', DEFAULT_UPLINK_CONVERTER)
        module = TBModuleLoader.import_module(self._connector_type, converter_class_name)

        if module:
            self.__log.debug('Converter %s for device %s - found!', converter_class_name, self.name)
            return module

        self.__log.error("Cannot find converter for %s device", self.name)
        return None

    @staticmethod
    def is_regex_pattern(pattern):
        return not re.fullmatch(pattern, pattern)

    async def __find_nodes(self, node_list, current_parent_node, nodes):
        assert len(node_list) > 0
        final = []

        children = await current_parent_node.get_children()
        for node in children:
            child_node = await node.read_browse_name()

            if re.fullmatch(re.escape(node_list[0]), child_node.Name) or node_list[0].split(':')[-1] == child_node.Name:
                new_nodes = [*nodes, f'{child_node.NamespaceIndex}:{child_node.Name}']
                if len(node_list) == 1:
                    final.append(new_nodes)
                else:
                    final.extend(await self.__find_nodes(node_list[1:], current_parent_node=node, nodes=new_nodes))

        return final

    async def find_nodes(self, node_pattern, current_parent_node=None, nodes=[]):
        node_list = node_pattern.split('\\.')

        if current_parent_node is None:
            current_parent_node = self.__client.nodes.root

            if len(node_list) > 0 and node_list[0].lower() == 'root':
                node_list = node_list[1:]

        return await self.__find_nodes(node_list, current_parent_node, nodes)

    async def find_node_name_space_index(self, path):
        if isinstance(path, str):
            path = path.split('\\.')

        # find unresolved nodes
        u_node_count = len(tuple(filter(lambda u_node: len(u_node.split(':')) < 2, path)))

        resolved = path[:-u_node_count]
        resolved_level = len(path) - u_node_count
        parent_node = await self.__client.nodes.root.get_child(resolved)

        unresolved = path[resolved_level:]
        return await self.__find_nodes(unresolved, current_parent_node=parent_node, nodes=resolved)

    async def _get_device_info_by_pattern(self, pattern, get_first=False):
        result = []

        search_result = re.search(r"\${([A-Za-z.:\\\d]+)}", pattern)
        if search_result:
            try:
                group = search_result.group(0)
                node_path = search_result.group(1)
            except IndexError:
                self.__log.error('Invalid pattern: %s', pattern)
                return result

            nodes = await self.find_nodes(node_path)
            self.__log.debug('Found device name nodes: %s', nodes)

            for node in nodes:
                try:
                    var = await self.__client.nodes.root.get_child(node)
                    value = await var.read_value()
                    result.append(pattern.replace(group, str(value)))
                except Exception as e:
                    self.__log.exception(e)
                    continue
        else:
            result.append(pattern)

        return result[0] if len(result) > 0 and get_first else result

    def __convert_sub_data(self):
        while not self.__stopped:
            if not self.__sub_data_to_convert.empty():
                sub_node, data = self.__sub_data_to_convert.get()

                for device in self.__device_nodes:
                    for section in ('attributes', 'timeseries'):
                        for node in device.values.get(section, []):
                            if node.get('id') == sub_node.__str__():
                                converter_data = device.converter_for_sub.convert({'section': section, 'key': node['key']},
                                                                                  data.monitored_item.Value)

                                if converter_data:
                                    self.__data_to_send.put(*converter_data)
                                    device.converter_for_sub.clear_data()
            else:
                sleep(.05)

    async def __scan_device_nodes(self):
        await self._create_new_devices()
        await self._load_devices_nodes()

    async def _create_new_devices(self):
        existing_devices = list(map(lambda dev: dev.name, self.__device_nodes))

        scanned_devices = []
        for device in self.__config.get('mapping', []):
            nodes = await self.find_nodes(device['deviceNodePattern'])
            self.__log.debug('Found devices: %s', nodes)

            device_names = await self._get_device_info_by_pattern(
                device.get('deviceInfo', {}).get('deviceNameExpression'))

            for device_name in device_names:
                scanned_devices.append(device_name)
                if device_name not in existing_devices:
                    for node in nodes:
                        converter = self.__load_converter(device)
                        device_type = await self._get_device_info_by_pattern(
                            device.get('deviceInfo', {}).get('deviceProfileExpression', 'default'),
                            get_first=True)
                        device_config = {**device, 'device_name': device_name, 'device_type': device_type}
                        self.__device_nodes.append(
                            Device(path=node, name=device_name, config=device_config,
                                   converter=converter(device_config, self.__log),
                                   converter_for_sub=converter(device_config,
                                                               self.__log) if self.__enable_subscriptions else None,
                                   logger=self.__log))

                        self.__log.info('Added device node: %s', device_name)

        for device_name in existing_devices:
            if device_name not in scanned_devices:
                await self.__reset_nodes(device_name)

        self.__log.debug('Device nodes: %s', self.__device_nodes)

    async def _load_devices_nodes(self):
        for device in self.__device_nodes:
            device.nodes = []
            for section in ('attributes', 'timeseries'):
                for node in device.values.get(section, []):
                    try:
                        path = node.get('qualified_path', node['path'])
                        if isinstance(path, str) and re.match(r"(ns=\d+;[isgb]=[^}]+)", path):
                            found_node = self.__client.get_node(path)
                        else:
                            if len(path[-1].split(':')) != 2:
                                qualified_path = await self.find_node_name_space_index(path)
                                if len(qualified_path) == 0:
                                    if node.get('valid', True):
                                        self.__log.warning('Node not found; device: %s, key: %s, path: %s',
                                                           device.name,
                                                           node['key'], node['path'])
                                        await self.__reset_node(node)
                                    continue
                                elif len(qualified_path) > 1:
                                    self.__log.warning(
                                        'Multiple matching nodes found; device: %s, key: %s, path: %s; %s',
                                        device.name,
                                        node['key'], node['path'], qualified_path)
                                node['qualified_path'] = qualified_path[0]
                                path = qualified_path[0]

                            found_node = await self.__client.nodes.root.get_child(path)

                        device.nodes.append({'node': found_node, 'key': node['key'], 'section': section})

                        if node.get('valid') is None or (node.get('valid') and not self.__enable_subscriptions):
                            if self.__enable_subscriptions and not node.get('sub_on', False) and not self.__stopped:
                                if self.__subscription is None:
                                    self.__subscription = await self.__client.create_subscription(1, SubHandler(
                                        self.__sub_data_to_convert, self.__log))
                                handle = await self.__subscription.subscribe_data_change(found_node)
                                node['subscription'] = handle
                                node['sub_on'] = True
                                node['id'] = found_node.nodeid.to_string()
                                self.__log.info("Subscribed on data change; device: %s, key: %s, path: %s",
                                                device.name,
                                                node['key'], node['id'])

                            node['valid'] = True
                    except ConnectionError as e:
                        raise e
                    except (BadNodeIdUnknown, BadConnectionClosed, BadInvalidState, BadAttributeIdInvalid,
                            BadCommunicationError, BadOutOfService, BadNoMatch, BadUnexpectedError,
                            UaStatusCodeErrors,
                            BadWaitingForInitialData):
                        if node.get('valid', True):
                            self.__log.warning('Node not found (2); device: %s, key: %s, path: %s', device.name,
                                               node['key'], node['path'])
                            await self.__reset_node(node)
                    except UaStatusCodeError as uae:
                        if node.get('valid', True):
                            self.__log.exception('Node status code error: %s', uae)
                            await self.__reset_node(node)
                    except Exception as e:
                        if node.get('valid', True):
                            self.__log.exception(e)
                            await self.__reset_node(node)

    async def __poll_nodes(self):
        all_nodes = [node_config['node'] for device in self.__device_nodes for node_config in device.nodes]

        if len(all_nodes) > 0:
            values = await self.__client.read_attributes(all_nodes)

            converted_nodes_count = 0
            for device in self.__device_nodes:
                nodes_count = len(device.nodes)
                device_values = values[converted_nodes_count:converted_nodes_count + nodes_count]
                converted_nodes_count += nodes_count
                converter_data = device.converter.convert(device.nodes, device_values)
                if converter_data:
                    self.__data_to_send.put(converter_data)

            self.__log.debug('Converted nodes values count: %s', converted_nodes_count)
        else:
            self.__log.info('No nodes to poll')

    def __send_data(self):
        while not self.__stopped:
            if not self.__data_to_send.empty():
                data = self.__data_to_send.get()
                self.statistics['MessagesReceived'] = self.statistics['MessagesReceived'] + 1
                self.__gateway.send_to_storage(self.get_name(), self.get_id(), data)
                self.statistics['MessagesSent'] = self.statistics['MessagesSent'] + 1
                self.__log.debug('Count data packs to ThingsBoard: %s', self.statistics['MessagesSent'])
            else:
                sleep(.05)

    async def get_shared_attr_node_id(self, path, result={}):
        try:
            q_path = await self.find_node_name_space_index(path)
            result['result'] = await self.__client.nodes.root.get_child(q_path[0])
        except Exception as e:
            result['error'] = e.__str__()

    def on_attributes_update(self, content):
        self.__log.debug(content)
        try:
            device = tuple(filter(lambda i: i.name == content['device'], self.__device_nodes))[0]

            for (key, value) in content['data'].items():
                for attr_update in device.config['attributes_updates']:
                    if attr_update['key'] == key:
                        result = {}
                        task = self.__loop.create_task(
                            self.get_shared_attr_node_id(
                                device.path + attr_update['value'].replace('\\', '').split('.'), result))

                        while not task.done():
                            sleep(.1)

                        if result.get('error'):
                            self.__log.error('Node not found! (%s)', result['error'])
                            return

                        node_id = result['result']
                        self.__loop.create_task(self.__write_value(node_id, value))
                        return
        except Exception as e:
            self.__log.exception(e)

    def server_side_rpc_handler(self, content):
        try:
            if content.get('data') is None:
                content['data'] = {'params': content['params'], 'method': content['method'], 'id': content['id']}

            rpc_method = content["data"].get("method")

            # check if RPC type is connector RPC (can be only 'get' or 'set')
            try:
                (connector_type, rpc_method_name) = rpc_method.split('_')
                if connector_type == self._connector_type:
                    rpc_method = rpc_method_name
                    content['device'] = content['params'].split(' ')[0].split('=')[-1]
            except (ValueError, IndexError):
                self.__log.error('Invalid RPC method name: %s', rpc_method)
            except AttributeError:
                pass

            if content.get('device'):
                # firstly check if a method is not service
                if rpc_method == 'set' or rpc_method == 'get':
                    full_path = ''
                    args_list = []
                    device = content.get('device')

                    try:
                        args_list = content['data']['params'].split(';')

                        if 'ns' in content['data']['params']:
                            full_path = ';'.join(
                                [item for item in (args_list[0:-1] if rpc_method == 'set' else args_list)])
                        else:
                            full_path = args_list[0].split('=')[-1]
                    except IndexError:
                        self.__log.error('Not enough arguments. Expected min 2.')
                        self.__gateway.send_rpc_reply(device=device,
                                                      req_id=content['data'].get('id'),
                                                      content={content['data'][
                                                                   'method']: 'Not enough arguments. Expected min 2.',
                                                               'code': 400})

                    result = {}
                    if rpc_method == 'get':
                        task = self.__loop.create_task(self.__read_value(full_path, result))

                        while not task.done():
                            sleep(.2)
                    elif rpc_method == 'set':
                        value = args_list[2].split('=')[-1]
                        task = self.__loop.create_task(self.__write_value(full_path, value, result))

                        while not task.done():
                            sleep(.2)

                    self.__gateway.send_rpc_reply(device=device,
                                                  req_id=content['data'].get('id'),
                                                  content={content['data']['method']: result})
                else:
                    device = tuple(filter(lambda i: i.name == content['device'], self.__device_nodes))[0]

                    for rpc in device.config['rpc_methods']:
                        if rpc['method'] == content["data"]['method']:
                            arguments_from_config = rpc["arguments"]
                            arguments = content["data"].get("params") if content["data"].get(
                                "params") is not None else arguments_from_config
                            method_name = content['data']['method']

                            try:
                                result = {}
                                task = self.__loop.create_task(
                                    self.__call_method(device.path, method_name, arguments, result))

                                while not task.done():
                                    sleep(.2)

                                self.__gateway.send_rpc_reply(content["device"],
                                                              content["data"]["id"],
                                                              {content["data"]["method"]: result, "code": 200})
                                self.__log.debug("method %s result is: %s", rpc['method'], result)
                            except Exception as e:
                                self.__log.exception(e)
                                self.__gateway.send_rpc_reply(content["device"], content["data"]["id"],
                                                              {"error": str(e), "code": 500})
                        else:
                            self.__log.error("Method %s not found for device %s", rpc_method, content["device"])
                            self.__gateway.send_rpc_reply(content["device"], content["data"]["id"],
                                                          {"error": "%s - Method not found" % rpc_method,
                                                           "code": 404})
            else:
                results = []
                for device in self.__device_nodes:
                    content['device'] = device.name

                    arguments = content['data']['params']["arguments"]

                    try:
                        result = {}
                        task = self.__loop.create_task(
                            self.__call_method(device.path, rpc_method, arguments, result))

                        while not task.done():
                            sleep(.2)

                        results.append(result)
                        self.__log.debug("method %s result is: %s", rpc_method, result)
                    except Exception as e:
                        self.__log.exception(e)
                        self.__gateway.send_rpc_reply(content["device"], content["data"]["id"],
                                                      {"error": str(e), "code": 500})

                return results
        except Exception as e:
            self.__log.exception(e)

    async def __write_value(self, path, value, result={}):
        try:
            var = path
            if isinstance(path, str):
                var = self.__client.get_node(path.replace('\\.', '.'))

            await var.write_value(value)
        except Exception as e:
            result['error'] = e.__str__()

    async def __read_value(self, path, result={}):
        try:
            var = self.__client.get_node(path)
            result['value'] = await var.read_value()
        except Exception as e:
            result['error'] = e.__str__()

    async def __call_method(self, path, method_name, arguments, result={}):
        try:
            var = await self.__client.nodes.root.get_child(path)
            method_id = '{}:{}'.format(var.nodeid.NamespaceIndex, method_name)
            result['result'] = await var.call_method(method_id, *arguments)
        except Exception as e:
            result['error'] = e.__str__()

    def update_converter_config(self, converter_name, config):
        self.__log.debug('Received remote converter configuration update for %s with configuration %s', converter_name,
                         config)
        for device in self.__device_nodes:
            if device.converter.__class__.__name__ == converter_name:
                device.config.update(config)
                device.load_values()
                self.__log.info('Updated converter configuration for: %s with configuration %s',
                                converter_name, device.config)

                for node_config in self.__config['mapping']:
                    if node_config['deviceNodePattern'] == device.config['deviceNodePattern']:
                        node_config.update(config)

                self.__gateway.update_connector_config_file(self.name, {'server': self.__server_conf,
                                                                        'mapping': self.__config.get('mapping', [])})


class SubHandler:
    def __init__(self, queue, logger):
        self.__log = logger
        self.__queue = queue

    def datachange_notification(self, node, _, data):
        self.__log.debug("New data change event %s %s", node, data)
        self.__queue.put((node, data))
