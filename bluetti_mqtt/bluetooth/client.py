import asyncio
from enum import Enum, auto, unique
import logging
import time
from typing import Optional, Union
from bleak import BleakClient, BleakError, BleakScanner
from bleak.exc import BleakDeviceNotFoundError
from bluetti_mqtt.core import DeviceCommand
from .exc import BadConnectionError, ModbusError, ParseError
from bleak.exc import BleakDBusError

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)


@unique
class ClientState(Enum):
    NOT_CONNECTED = auto()
    CONNECTED = auto()
    READY = auto()
    PERFORMING_COMMAND = auto()
    COMMAND_ERROR_WAIT = auto()
    DISCONNECTING = auto()


class BluetoothClient:
    RESPONSE_TIMEOUT = 5
    WRITE_UUID = '0000ff02-0000-1000-8000-00805f9b34fb'
    NOTIFY_UUID = '0000ff01-0000-1000-8000-00805f9b34fb'
    DEVICE_NAME_UUID = '00002a00-0000-1000-8000-00805f9b34fb'

    name: Union[str, None]
    current_command: DeviceCommand
    notify_future: asyncio.Future
    notify_response: bytearray

    def __init__(self, address: str, name: Optional[str] = None):
        self.address = address
        self.state = ClientState.NOT_CONNECTED
        self.name = name
        self.client = BleakClient(self.address)
        self.last_warning_time = 0
        self.MAX_RETRIES = 5
        self.command_queue = asyncio.Queue()
        self.notify_future = None
        self.loop = asyncio.get_running_loop()
        self.logger = logging.getLogger(f"BluetoothClient-{self.address}")
        self.logger.info(f"Initializing BluetoothClient for {self.address} with name {self.name}")
        self.logger.debug(f"Starting BluetoothClient for {self.address}")
        
    @property
    def is_ready(self):
        return self.state == ClientState.READY or self.state == ClientState.PERFORMING_COMMAND

    async def perform(self, cmd: DeviceCommand):
        future = self.loop.create_future()
        await self.command_queue.put((cmd, future))
        return future

    async def perform_nowait(self, cmd: DeviceCommand):
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
                        self.logger.debug(f"Unexpected current state {self.state}")
                        self.state = ClientState.NOT_CONNECTED
                except BleakDBusError as e:
                    self.logger.error(f"DBus error encountered: {e}. Retrying...")
                    await asyncio.sleep(5)  # Grace period before retry
                except Exception as e:
                    self.logger.exception(f"Unhandled error: {e}. Exiting loop.")
                    break
        finally:
            # Ensure disconnection on exit
            if self.client and self.client.is_connected:
                await self.client.disconnect()

    async def _connect(self):
        """Establish connection to the bluetooth device"""
        retries = 0
        delay = 5  # Initial retry delay in seconds
        max_delay = 60  # Maximum delay of 1 minute

        while True:
            if self.client.is_connected:
                current_time = time.time()
                if current_time - self.last_warning_time > delay:
                    self.logger.debug(f"Client {self.address} is already connected. Skipping connection attempt.")
                    self.last_warning_time = current_time
                    await asyncio.sleep(delay) 

                    # Implement exponential backoff logic with cap
                    delay = min(delay * 2, max_delay)
                return

            try:
                self.logger.debug(f"Scanning for device {self.address}...")
                devices = await BleakScanner.discover(timeout=20)
                self.logger.debug(f"Discovered devices: {[f'{d.name} ({d.address})' for d in devices]}")
                device_found = any(d.address.lower() == self.address.lower() for d in devices)
                if not device_found:
                    self.logger.debug(f"Device with address {self.address} not found. Check power and range.")
                    retries += 1
                    await asyncio.sleep(delay)
                    continue

                self.logger.debug(f"Attempting to connect to {self.address}...")
                await self.client.connect()
                self.state = ClientState.CONNECTED
                self.logger.info(f"Successfully connected to {self.address}.")

                # Reset delay after a successful connection
                delay = 5
                return
            except BleakDBusError as e:
                if "InProgress" in str(e):
                    self.logger.debug(f"Connection already in progress [{self.address}]. Retrying in {delay}s")
                    # Try to disconnect if delay is over 30 seconds:
                    if delay > 30:
                        self.logger.warning(f"Disconnecting from {self.address} due to connection in progress.")
                        self.state = ClientState.DISCONNECTING
                else:
                    self.logger.error(f"BleakDBusError for {self.address}")
                    self.logger.debug(f"{e}")
                retries += 1
                await asyncio.sleep(delay)
            except BleakDeviceNotFoundError:
                logging.debug(f'Error connecting to device {self.address}: Not found')
            except (BleakError, EOFError, asyncio.TimeoutError) as e:
                logging.exception(f'Error connecting to device {self.address}')
                self.logger.debug(f'{e}')
                await asyncio.sleep(1)
            except Exception as e:
                self.logger.error(f"Unexpected error connecting to {self.address}")
                self.logger.debug(f"{e}")
                retries += 1
                await asyncio.sleep(delay)

            # Adjust the delay for the next attempt
            delay = min(delay * 2, max_delay)

        self.logger.error(f"Exceeded maximum retries ({self.MAX_RETRIES}) for {self.address}. Connection failed.")

    # async def _restart_discovery(self):
    #     """Restart the Bluetooth discovery process."""
    #     try:
    #         from bleak import BleakScanner

    #         self.logger.info("Restarting Bluetooth discovery...")
    #         await BleakScanner.stop()
    #         await asyncio.sleep(1)  # Give time for the stop to complete
    #         await BleakScanner.start()
    #         self.logger.info("Bluetooth discovery restarted.")
    #     except Exception as e:
    #         self.logger.error(f"Failed to restart Bluetooth discovery: {e}")

    async def _get_name(self):
        """Get device name, which can be parsed for type"""
        try:
            name = await self.client.read_gatt_char(self.DEVICE_NAME_UUID)
            self.name = name.decode('ascii')
            logging.info(f'Device {self.address} has name: {self.name}')
        except BleakError:
            logging.exception(f'Error retrieving device name {self.address}:')
            self.state = ClientState.DISCONNECTING

    async def _retry_connection(self, delay=10):
        self.logger.info(f"Retrying connection to {self.address} in {delay} seconds...")
        await asyncio.sleep(delay)
        try:
            await self._connect()
        except Exception as e:
            self.logger.error(f"Retry failed for {self.address}: {e}")

    async def _start_listening(self):
        """Register for command response notifications"""
        try:
            await self.client.start_notify(
                self.NOTIFY_UUID,
                self._notification_handler)
            self.state = ClientState.READY
        except BleakError:
            self.state = ClientState.DISCONNECTING

    async def _perform_command(self):
        cmd, cmd_future = await self.command_queue.get()
        retries = 0
        while retries < 5:
            try:
                # Prepare to make request
                self.state = ClientState.PERFORMING_COMMAND
                self.current_command = cmd
                self.notify_future = self.loop.create_future()
                self.notify_response = bytearray()

                # Make request
                await self.client.write_gatt_char(
                    self.WRITE_UUID,
                    bytes(self.current_command))

                # Wait for response
                res = await asyncio.wait_for(
                    self.notify_future,
                    timeout=self.RESPONSE_TIMEOUT)
                if cmd_future:
                    cmd_future.set_result(res)

                # Success!
                self.state = ClientState.READY
                break
            except ParseError:
                # For safety, wait the full timeout before retrying again
                self.state = ClientState.COMMAND_ERROR_WAIT
                retries += 1
                await asyncio.sleep(self.RESPONSE_TIMEOUT)
            except asyncio.TimeoutError:
                self.state = ClientState.COMMAND_ERROR_WAIT
                retries += 1
            except ModbusError as err:
                if cmd_future:
                    cmd_future.set_exception(err)

                # Don't retry
                self.state = ClientState.READY
                break
            except (BleakError, EOFError, BadConnectionError) as err:
                if cmd_future:
                    cmd_future.set_exception(err)

                self.state = ClientState.DISCONNECTING
                break

        if retries == 5:
            err = BadConnectionError('too many retries')
            if cmd_future:
                cmd_future.set_exception(err)
            self.state = ClientState.DISCONNECTING

        self.command_queue.task_done()

    async def _disconnect(self):
        await self.client.disconnect()
        logging.warn(f'Delayed reconnect to {self.address} after error')
        await asyncio.sleep(5)
        self.state = ClientState.NOT_CONNECTED

    def _notification_handler(self, _sender: int, data: bytearray):
        if self.notify_future and not self.notify_future.done():
            # Ignore notifications we don't expect
            if not self.notify_future or self.notify_future.done():
                return

            # If something went wrong, we might get weird data.
            if data == b'AT+NAME?\r' or data == b'AT+ADV?\r':
                err = BadConnectionError('Got AT+ notification')
                self.notify_future.set_exception(err)
                return

            # Save data
            self.notify_response.extend(data)

            if len(self.notify_response) == self.current_command.response_size():
                if self.current_command.is_valid_response(self.notify_response):
                    self.notify_future.set_result(self.notify_response)
                else:
                    self.notify_future.set_exception(ParseError('Failed checksum'))
            elif self.current_command.is_exception_response(self.notify_response):
                # We got a MODBUS command exception
                msg = f'MODBUS Exception {self.current_command}: {self.notify_response[2]}'
                self.notify_future.set_exception(ModbusError(msg))