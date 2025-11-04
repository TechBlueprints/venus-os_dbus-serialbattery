#!/usr/bin/env python3
# Minimal LLT/JBD fast BLE probe without D-Bus registration

import logging
import os
import sys
from time import sleep, time

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
EXT_DIR = os.path.join(BASE_DIR, "ext")
if EXT_DIR not in sys.path:
    sys.path.insert(1, EXT_DIR)

from bms.lltjbd_fastble import LltJbd_FastBle


def main(addr: str, seconds: int = 90) -> int:
    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s:%(name)s:%(message)s")
    addr = addr.strip()
    port = "ble_" + addr.replace(":", "").lower()
    logging.info(f"Starting JBD fast BLE probe for {addr} (port={port}) for ~{seconds}s")

    bat = LltJbd_FastBle(port=port, baud=9600, address=addr)

    if not bat.test_connection():
        logging.error("test_connection() returned False")
        return 2

    sleep(3)
    started = time()
    frames_ok = 0
    gen_reads = 0
    cell_reads = 0
    while time() - started < seconds:
        try:
            ok_gen = bat.read_gen_data()
            gen_reads += 1
            if ok_gen:
                frames_ok += 1
                logging.info(
                    f"GEN ok: V={bat.voltage}V I={bat.current}A SoC={bat.soc}% T1={bat.temperature_1}C MOS={bat.temperature_mos}C"
                )
            ok_cells = bat.read_cell_data()
            cell_reads += 1
            if ok_cells and bat.cells:
                populated = sum(1 for c in bat.cells if c.voltage is not None)
                first = bat.cells[0].voltage if bat.cells and bat.cells[0].voltage is not None else None
                logging.info(f"CELLS ok: count={bat.cell_count} populated={populated} first={first}V")
        except KeyboardInterrupt:
            break
        except Exception:
            pass
        sleep(2)

    logging.info(f"Probe finished: frames_ok={frames_ok}, gen_reads={gen_reads}, cell_reads={cell_reads}")
    try:
        bat.run = False
        sleep(1)
    except Exception:
        pass
    return 0 if frames_ok > 0 else 1


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: jbd_fast_probe.py <BLE_ADDR> [seconds]", file=sys.stderr)
        sys.exit(64)
    addr = sys.argv[1]
    seconds = int(sys.argv[2]) if len(sys.argv) > 2 else 90
    sys.exit(main(addr, seconds))


