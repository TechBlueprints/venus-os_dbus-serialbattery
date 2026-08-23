# -*- coding: utf-8 -*-
"""bleak-connection-manager v2: the embeddable bleak catcher.

Two layers, deliberately separable:

- ``bleak_connection_manager.claims``: the bt-claims file convention
  (stdlib only, no bleak, vendorable verbatim). Usable standalone by any
  service that wants adapter coordination without the bleak machinery.
- ``bleak_connection_manager.catcher``: the process-wide bleak client
  rebinding layer (requires bleak), exposed lazily here so importing the
  package - or the claims module - never drags bleak in.
- ``bleak_connection_manager.validators``: post-connect validators for the
  routed client (stdlib only, duck-typed on the client, so it imports
  without bleak too).
"""

__version__ = "2.0.0.dev0"

_CATCHER_EXPORTS = (
    "install_bleak_catcher",
    "uninstall_bleak_catcher",
    "BLEConnection",
    "BLEConnectionWithServiceCache",
    "BLEScanner",
    "OutOfConnectionSlotsError",
    "ConnectionValidationError",
    "parse_adapter_entries",
    "rewrite_adapter_config",
)

_VALIDATOR_EXPORTS = (
    "validate_gatt_services",
    "validate_char_exists",
    "validate_read_char",
    "tolerate_late_gatt",
    "refresh_services",
)

__all__ = list(_CATCHER_EXPORTS) + list(_VALIDATOR_EXPORTS) + ["claims", "validators", "reset_adapter"]


def __getattr__(name):
    if name in _CATCHER_EXPORTS:
        from bleak_connection_manager import catcher

        return getattr(catcher, name)
    if name in _VALIDATOR_EXPORTS:
        from bleak_connection_manager import validators

        return getattr(validators, name)
    if name == "reset_adapter":
        # claims-gated adapter recovery; stdlib unless the reset actually
        # runs, so importing it never drags bleak in
        from bleak_connection_manager import recovery

        return recovery.reset_adapter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
