#!/usr/bin/env python3
"""Retags a pure-python wheel with a specific platform tag and Root-Is-Purelib: false."""

import os
import re
import sys
import zipfile

def retag_wheel(whl_path: str, platform_tag: str, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    with zipfile.ZipFile(whl_path, "r") as zin:
        whl_name = os.path.basename(whl_path)
        parts = whl_name[:-4].split("-")
        dist, version = parts[0], parts[1]
        py_tag = "py3"
        abi_tag = "none"
        new_whl_name = f"{dist}-{version}-{py_tag}-{abi_tag}-{platform_tag}.whl"
        out_path = os.path.join(out_dir, new_whl_name)

        with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                data = zin.read(item.filename)
                if item.filename.endswith(".dist-info/WHEEL"):
                    text = data.decode("utf-8")
                    text = re.sub(r"Root-Is-Purelib:\s*true", "Root-Is-Purelib: false", text)
                    text = re.sub(r"Tag:\s*.*", f"Tag: {py_tag}-{abi_tag}-{platform_tag}", text)
                    data = text.encode("utf-8")
                zout.writestr(item, data)
    print(f"Created: {out_path}")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print(f"Usage: {sys.argv[0]} <whl_path> <platform_tag> <out_dir>")
        sys.exit(1)
    retag_wheel(sys.argv[1], sys.argv[2], sys.argv[3])
