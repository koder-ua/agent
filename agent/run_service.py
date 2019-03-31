#!/usr/bin/env python3.5
import os
import sys
from pathlib import Path


if __name__ == "__main__":
    service_path = Path(sys.argv[1])

    environ = os.environ.copy()
    pypath = environ.get("PYTHONPATH", "")

    if (service_path / 'libs').is_dir():
         pypath += ":" + str(service_path / 'libs')

    pypath += ":" + str(service_path)
    environ["PYTHONPATH"] = pypath

    if (service_path / 'python').is_dir():
        environ['PYTHONHOME'] = str(service_path / 'python')
        pythonbins = list((service_path / 'python').glob("python3.[789]"))
        assert len(pythonbins) == 1
        pythonbin = str(pythonbins[0])
    else:
        pythonbin = sys.executable

    cert_folder = service_path / 'certs'

    sys.stdout.flush()
    sys.stderr.flush()

    os.execve(pythonbin,
              [pythonbin, "-m", "agent.server", "server",
               "--cert", str(cert_folder / "ssl_cert.cert"),
               "--key", str(cert_folder / "ssl_cert.key"),
               "--api-key", str(cert_folder / "api.key")],
              environ)
