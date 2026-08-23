# -*- coding: utf-8 -*-
"""Post-connect validators for the routed client.

A validator is `async (client) -> bool`, handed the connected client and
answering whether the link is actually usable. The catcher treats a False
(or a raise) as a failed connect: the link is torn down, the adapter is
penalised, and ConnectionValidationError - a BleakError - reaches
bleak-retry-connector's retry loop like any other connect failure. Supply
one per client (`validate_connection=` in the client kwargs, which
establish_connection passes through) or process wide
(`install_bleak_catcher(validate_connection=...)`).

These are the v1 built-ins, ordered weakest to strongest:

1. :func:`validate_gatt_services` - GATT services non-empty.
2. :func:`validate_char_exists` - a specific characteristic UUID exists.
3. :func:`validate_read_char` - actually reads from a characteristic.

Each catches its own exceptions and returns False rather than raising, so
composition stays predictable. :func:`tolerate_late_gatt` wraps any of them
for chips that register their vendor services seconds after
ServicesResolved.

Stdlib only, duck-typed on the client: importing this module never drags
bleak in.
"""

import asyncio
import logging

logger = logging.getLogger(__name__)

# v1's escalating waits for late GATT registration (2s, then 4s, then 8s -
# 14s total), from the field behaviour of Telink-based chips that announce
# ServicesResolved with only the Generic Attribute service present.
LATE_GATT_WAITS = (2.0, 4.0, 8.0)


async def validate_gatt_services(client):
    """Validate that GATT service discovery actually produced services.

    Catches the connect that succeeds with an empty service table: GATT
    discovery failed silently, or ServicesResolved fired ahead of it.
    """
    if not client.services:
        logger.debug(f"validate_gatt_services: GATT services empty for {client.address}")
        return False
    return True


def validate_char_exists(uuid):
    """Build a validator that requires characteristic `uuid` to be present.

    Everything validate_gatt_services catches, plus partial GATT
    resolution: services arrived, but not the one the caller needs.
    """
    target = uuid.lower()

    async def _validator(client):
        if not client.services:
            logger.warning(
                f"validate_char_exists({uuid}): GATT services empty for {client.address} - "
                "ServicesResolved fired but no services discovered"
            )
            return False
        services = []
        chars = []
        for service in client.services:
            services.append(service.uuid)
            for char in service.characteristics:
                if char.uuid.lower() == target:
                    return True
                chars.append(char.uuid)
        logger.warning(
            f"validate_char_exists({uuid}): characteristic NOT FOUND for {client.address} - "
            f"GATT has {len(services)} services, {len(chars)} characteristics. "
            f"Services: {', '.join(services)} | Characteristics: {', '.join(chars)}"
        )
        return False

    _validator.__doc__ = f"Validate that characteristic {uuid} exists in GATT services."
    return _validator


def validate_read_char(uuid, timeout=5.0):
    """Build a validator that reads `uuid` to prove the link end to end.

    The strongest built-in: services non-empty, the characteristic present,
    and a real read that returns within `timeout`. A phantom connection or
    a dead HCI handle fails here where the weaker checks pass.
    """
    target = uuid.lower()

    async def _validator(client):
        if not client.services:
            logger.debug(f"validate_read_char({uuid}): GATT services empty for {client.address}")
            return False
        found = False
        for service in client.services:
            for char in service.characteristics:
                if char.uuid.lower() == target:
                    found = True
                    break
            if found:
                break
        if not found:
            logger.debug(f"validate_read_char({uuid}): characteristic not found for {client.address}")
            return False
        try:
            data = await asyncio.wait_for(client.read_gatt_char(uuid), timeout=timeout)
        except asyncio.TimeoutError:
            logger.debug(f"validate_read_char({uuid}): read timed out after {timeout:.1f}s for {client.address}")
            return False
        except Exception:
            logger.debug(f"validate_read_char({uuid}): read failed for {client.address}", exc_info=True)
            return False
        logger.debug(f"validate_read_char({uuid}): read {len(data)} bytes from {client.address}")
        return True

    _validator.__doc__ = f"Validate connection by reading characteristic {uuid}."
    return _validator


async def refresh_services(client):
    """Re-read the GATT service table from the backend, bypassing the cache.

    bleak resolves services once and caches the collection on the backend,
    so a validator that re-checks without this sees the same empty table
    forever. Clearing the backend's own attribute (not the client property)
    makes the next resolve rebuild from BlueZ's current state. Returns
    whether the re-read ran; best effort, since it reaches into the
    backend.
    """
    backend = getattr(client, "_backend", None)
    get_services = getattr(backend, "_get_services", None) or getattr(backend, "get_services", None)
    if get_services is None:
        logger.debug("refresh_services: backend exposes no service re-read, skipping")
        return False
    try:
        backend.services = None
    except Exception:
        logger.debug("refresh_services: could not clear cached services", exc_info=True)
    try:
        try:
            await get_services(dangerous_use_bleak_cache=False)
        except TypeError:
            await get_services()
    except Exception:
        logger.warning("refresh_services: GATT re-read raised", exc_info=True)
        return False
    return True


async def _safe(validator, client):
    """Run a validator the way the catcher does: a raise counts as False."""
    try:
        return bool(await validator(client))
    except Exception:
        logger.debug("validate_connection raised, treating as failed", exc_info=True)
        return False


def tolerate_late_gatt(validator, waits=LATE_GATT_WAITS):
    """Wrap a validator so late-registering GATT services still pass.

    Some chips (Telink-based ones notably) report ServicesResolved with
    only the Generic Attribute service registered; the vendor services
    arrive seconds later over InterfacesAdded or a Service Changed
    indication. This retries `validator` after each wait in `waits`,
    re-reading the service table from BlueZ in between, and gives up early
    if the link drops meanwhile.

    v1 applied these waits to every validator implicitly. Here it is
    explicit, because the catcher itself never retries - wrap the validator
    where you want v1's behaviour::

        validate_connection=tolerate_late_gatt(validate_char_exists(UUID))
    """
    waits = tuple(waits)

    async def _validator(client):
        if await _safe(validator, client):
            return True
        cumulative = 0.0
        for wait in waits:
            cumulative += wait
            logger.info(
                f"BLE [{client.address}]: validation failed, waiting {wait:.0f}s for late GATT services "
                f"({cumulative:.0f}s cumulative)"
            )
            await asyncio.sleep(wait)
            if not await refresh_services(client):
                # is_connected alone is not evidence of a dropped link: after
                # an adapter reset or a device object being removed and
                # re-added, BlueZ sends no property signal and bleak's cached
                # view is stranded False while GATT still works (field
                # 2026-08-22, and the easytouch driver's _exchange() has
                # routed around the same property since v1). A rejection here
                # disconnects the link, releases its claims and rotates the
                # radio, so it takes the real failure - a GATT re-read that
                # did not run - corroborated by the property, before
                # abandoning.
                if not client.is_connected:
                    logger.info(f"BLE [{client.address}]: link dropped during GATT retry wait, abandoning re-validation")
                    return False
                continue
            if await _safe(validator, client):
                logger.info(f"BLE [{client.address}]: late GATT services appeared after {cumulative:.0f}s, validation passed")
                return True
        return False

    return _validator
