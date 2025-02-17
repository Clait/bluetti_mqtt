import asyncio
from enum import Enum, auto, unique
import logging
from typing import Optional, Union
from bleak import BleakClient, BleakError, BleakScanner
from bleak.exc import BleakDeviceNotFoundError, BleakDBusError
from bluetti_mqtt.core import DeviceCommand
from .exc import BadConnectionError, ModbusError, ParseError


@unique
class ClientState(Enum):
    NOT_CONNECTED = auto()
    CONNECTED = auto()
    READY = auto()
    PERFORMING_COMMAND = auto()
    COMMAND_ERROR_WAIT = auto()
    DISCONNECTING = auto()


class BluetoothClient:
    RESPONSE_TIMEOUT: int = 5
    WRITE_UUID: str = '0000ff02-0000-1000-8000-00805f9b34fb'
    NOTIFY_UUID: str = '0000ff01-0000-1000-8000-00805f9b34fb'
    DEVICE_NAME_UUID: str = '00002a00-0000-1000-8000-00805f9b34fb'

    def __init__(self, address: str, name: Optional[str] = None):
        self.address = address
        self.state: ClientState = ClientState.NOT_CONNECTED
        self.name: Optional[str] = name
        self.client: BleakClient = BleakClient(self.address)
        self.last_warning_time: float = 0
        self.MAX_RETRIES: int = 5
        self.command_queue: asyncio.Queue = asyncio.Queue()
        self.notify_future: Optional[asyncio.Future] = None
        self.loop = asyncio.get_running_loop()
        self.logger = logging.getLogger(f"BluetoothClient-{self.address}")
        self.logger.info(f"Initializing BluetoothClient for {self.address} with name {self.name}")

    @property
    def is_ready(self) -> bool:
        return self.state in {ClientState.READY, ClientState.PERFORMING_COMMAND}

    async def _with_retries(self, coro, max_retries: int = 5, delay: float = 5, backoff: float = 2):
        retries = 0
        while retries < max_retries:
            try:
                return await coro()
            except (BleakError, asyncio.TimeoutError, BadConnectionError) as e:
                retries += 1
                self.logger.warning(f"Retry {retries}/{max_retries} failed: {e}")
                await asyncio.sleep(delay)
                delay *= backoff
        self.logger.error(f"Exceeded retries ({max_retries}). Operation failed.")
        raise BadConnectionError(f"Failed after {max_retries} retries.")

    async def perform(self, cmd: DeviceCommand) -> asyncio.Future:
        future: asyncio.Future = self.loop.create_future()
        await self.command_queue.put((cmd, future))
        return future

    async def perform_nowait(self, cmd: DeviceCommand) -> None:
        await self.command_queue.put((cmd, None))

    async def run(self):
        try:
            while True:
                try:
                    if self.state == ClientState.NOT_CONNECTED:
                        await self._connect()
                    elif self.state == ClientState.CONNECTED:
                        if not self.name:
                            await self._get_name()
                        else:
                            await self._start_listening()
                    elif self.state == ClientState.READY:
                        await self._perform_command()
                    elif self.state == ClientState.DISCONNECTING:
                        await self._disconnect()
                    else:
                        self.logger.warning(f"Unexpected state: {self.state}")
                        self.state = ClientState.NOT_CONNECTED
                except BleakDBusError as e:
                    self.logger.error(f"DBus error encountered: {e}. Retrying...")
                    await asyncio.sleep(5)
                except Exception as e:
                    self.logger.exception(f"Unhandled error: {e}. Exiting loop.")
                    break
        finally:
            if self.client and self.client.is_connected:
                await self.client.disconnect()

    async def _connect(self):
        async def connect_attempt():
            if self.client.is_connected:
                self.logger.warning(f"Client {self.address} is already connected.")
                return
            self.logger.info(f"Attempting to connect to {self.address}...")
            await self.client.connect()
            self.state = ClientState.CONNECTED
            self.logger.info(f"Successfully connected to {self.address}.")

        await self._with_retries(connect_attempt, self.MAX_RETRIES)

    async def _disconnect(self):
        try:
            if self.client.is_connected:
                self.logger.info(f"Disconnecting from {self.address}...")
                await self.client.disconnect()
        except BleakError as e:
            self.logger.error(f"Error during disconnect: {e}")
        finally:
            self.state = ClientState.NOT_CONNECTED

    async def _get_name(self):
        try:
            name = await self.client.read_gatt_char(self.DEVICE_NAME_UUID)
            self.name = name.decode('ascii')
            self.logger.info(f"Device {self.address} has name: {self.name}")
        except BleakError as e:
            self.logger.error(f"Error retrieving device name: {e}")
            self.state = ClientState.DISCONNECTING

    async def _start_listening(self):
        try:
            await self.client.start_notify(self.NOTIFY_UUID, self._notification_handler)
            self.state = ClientState.READY
        except BleakError as e:
            self.logger.error(f"Failed to start listening: {e}")
            self.state = ClientState.DISCONNECTING

    async def _perform_command(self):
        cmd, cmd_future = await self.command_queue.get()
        async def command_attempt():
            self.state = ClientState.PERFORMING_COMMAND
            self.current_command = cmd
            self.notify_future = self.loop.create_future()
            await self.client.write_gatt_char(self.WRITE_UUID, bytes(self.current_command))
            res = await asyncio.wait_for(self.notify_future, timeout=self.RESPONSE_TIMEOUT)
            if cmd_future:
                cmd_future.set_result(res)
            self.state = ClientState.READY

        try:
            await self._with_retries(command_attempt, max_retries=5)
        except Exception as e:
            self.logger.error(f"Command execution failed: {e}")
            if cmd_future:
                cmd_future.set_exception(e)
            self.state = ClientState.DISCONNECTING
        finally:
            self.command_queue.task_done()

    def _notification_handler(self, _sender: int, data: bytearray):
        if self.notify_future and not self.notify_future.done():
            self.notify_response.extend(data)
            if len(self.notify_response) == self.current_command.response_size():
                if self.current_command.is_valid_response(self.notify_response):
                    self.notify_future.set_result(self.notify_response)
                else:
                    self.notify_future.set_exception(ParseError("Invalid response checksum"))
