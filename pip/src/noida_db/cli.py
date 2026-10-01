import os
import subprocess
import sys
from importlib.resources import files

def main():
    binname = "noida-db.exe" if os.name == "nt" else "noida-db"
    binpath = files("noida_db").joinpath(binname)
    if not binpath.is_file():
        print(f"noida-db: binary not found at {binpath}", file=sys.stderr)
        sys.exit(1)
    os.chmod(str(binpath), 0o755)
    sys.exit(subprocess.call([str(binpath), *sys.argv[1:]]))

if __name__ == "__main__":
    main()
