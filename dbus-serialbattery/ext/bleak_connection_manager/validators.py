"""Built-in connection validators for use with ``establish_connection()``.

Each validator conforms to the ``Callable[[BleakClient], Awaitable[bool]]``
signature expected by the ``validate_connection`` parameter.  Pick the
level of validation that suits your use case, or write your own.

Validators are ordered from weakest to strongest:

1. :func:`validate_gatt_services` -- GATT services non-empty.
2. :func:`validate_char_exists` -- a specific characteristic UUID exists.
3. :func:`validate_read_char` -- actually reads from a characteristic.

All validators catch exceptions internally and return ``False`` on any
failure -- they never propagate exceptions to the retry loop.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

from bleak import BleakClient

_LOGGER = logging.getLogger(__name__)


async def validate_gatt_services(client: BleakClient) -> bool:
    """Validate that GATT service discovery completed.

    Returns ``True`` if ``client.services`` is non-empty.  This catches:

    - **Stuck State 9**: Connect succeeded but GATT discovery failed
      silently -- ``client.services`` is empty.
    - **Stuck State 17**: GATT resolution race -- connect returned
      before service discovery finished on slow BLE chips.

    Usage::

        client = await establish_connection(
            BleakClient, device,
            validate_connection=validate_gatt_services,
        )
    """
    if not client.services:
        _LOGGER.debug(
            "validate_gatt_services: GATT services empty for %s",
            client.address,
        )
        return False
    return True


def validate_char_exists(
    uuid: str,
) -> Callable[[BleakClient], Awaitable[bool]]:
    """Create a validator that checks for a specific characteristic UUID.

    Returns a validator function that confirms GATT services are
    non-empty *and* that the given characteristic UUID is present.
    This catches everything :func:`validate_gatt_services` catches,
    plus partial GATT resolution where the needed characteristic
    hasn't appeared yet.

    Parameters
    ----------
    uuid:
        The characteristic UUID to look for (case-insensitive).

    Usage::

        client = await establish_connection(
            BleakClient, device,
            validate_connection=validate_char_exists(
                "6e400003-b5a3-f393-e0a9-e50e24dcca9e"
            ),
        )
    """
    target = uuid.lower()

    async def _validator(client: BleakClient) -> bool:
        if not client.services:
            _LOGGER.warning(
                "validate_char_exists(%s): GATT services empty for %s — "
                "ServicesResolved fired but no services discovered",
                uuid,
                client.address,
            )
            return False

        for service in client.services:
            for char in service.characteristics:
                if char.uuid.lower() == target:
                    return True

        found_services = []
        found_chars = []
        for service in client.services:
            found_services.append(service.uuid)
            for char in service.characteristics:
                found_chars.append(char.uuid)

        _LOGGER.warning(
            "validate_char_exists(%s): characteristic NOT FOUND for %s — "
            "GATT has %d services, %d characteristics. "
            "Services: %s | Characteristics: %s",
            uuid,
            client.address,
            len(found_services),
            len(found_chars),
            ", ".join(found_services),
            ", ".join(found_chars),
        )
        return False

    _validator.__doc__ = (
        f"Validate that characteristic {uuid} exists in GATT services."
    )
    return _validator


def validate_read_char(
    uuid: str,
    timeout: float = 5.0,
) -> Callable[[BleakClient], Awaitable[bool]]:
    """Create a validator that reads from a characteristic to prove liveness.

    Returns a validator function that:

    1. Confirms GATT services are non-empty.
    2. Confirms the characteristic UUID exists.
    3. Performs an actual ``read_gatt_char()`` to verify the connection
       is live end-to-end.

    This is the strongest built-in validator.  It catches everything
    the weaker validators catch, plus:

    - **Stuck State 1**: Phantom connections adopted as "connected" --
      the read will fail with "Not connected".
    - **Stuck State 2**: Dead HCI handle -- the read will fail with
      a transport error.
    - **Stuck State 5**: Zombie connections (if used during initial
      validation) -- the read may hang or fail.

    Parameters
    ----------
    uuid:
        The characteristic UUID to read from (must support Read).
    timeout:
        Maximum seconds to wait for the read operation.

    Usage::

        client = await establish_connection(
            BleakClient, device,
            validate_connection=validate_read_char(
                "00002a19-0000-1000-8000-00805f9b34fb"  # Battery Level
            ),
        )
    """
    import asyncio

    target = uuid.lower()

    async def _validator(client: BleakClient) -> bool:
        if not client.services:
            _LOGGER.debug(
                "validate_read_char(%s): GATT services empty for %s",
                uuid,
                client.address,
            )
            return False

        char_found = False
        for service in client.services:
            for char in service.characteristics:
                if char.uuid.lower() == target:
                    char_found = True
                    break
            if char_found:
                break

        if not char_found:
            _LOGGER.debug(
                "validate_read_char(%s): characteristic not found for %s",
                uuid,
                client.address,
            )
            return False

        try:
            data = await asyncio.wait_for(
                client.read_gatt_char(uuid),
                timeout=timeout,
            )
            _LOGGER.debug(
                "validate_read_char(%s): read %d bytes from %s",
                uuid,
                len(data),
                client.address,
            )
            return True
        except asyncio.TimeoutError:
            _LOGGER.debug(
                "validate_read_char(%s): read timed out after %.1f s for %s",
                uuid,
                timeout,
                client.address,
            )
            return False
        except Exception:
            _LOGGER.debug(
                "validate_read_char(%s): read failed for %s",
                uuid,
                client.address,
                exc_info=True,
            )
            return False

    _validator.__doc__ = (
        f"Validate connection by reading characteristic {uuid}."
    )
    return _validator
