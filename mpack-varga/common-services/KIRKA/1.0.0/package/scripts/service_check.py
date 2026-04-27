"""Standalone service-check script for the KIRKA service.

Ambari requires a separate `service_check.py` (with its own `Script` subclass) for the
SERVICE_CHECK action invoked from the UI / REST API — a top-level `<commandScript>` in
metainfo.xml is not enough on its own.

The probe hits `/actuator/health/liveness` on each `KIRKA_SERVER` host and fails if the
response is anything other than HTTP 200. We deliberately keep this probe trivial so the
status reflects whether the JVM is up and the Spring context loaded — deeper readiness
(HBase reachable, Ranger policies fetched) belongs to dedicated alerts, not to the
service-check button.
"""

import sys

try:
    # Python 3 path on newer Ambari agents
    from urllib.request import urlopen
    from urllib.error import URLError
except ImportError:  # pragma: no cover  (Ambari 2.7 still ships Python 2 in places)
    from urllib2 import urlopen, URLError

from resource_management import *


class KirkaServiceCheck(Script):

    def service_check(self, env):
        import params
        env.set_params(params)

        url = format("http://localhost:{kirka_port}/actuator/health/liveness")
        Logger.info(format("Probing Kirka liveness at {url}"))
        try:
            response = urlopen(url, timeout=10)
            code = response.getcode()
        except URLError as e:
            raise Fail(format("Kirka liveness check failed: {e}"))
        if code != 200:
            raise Fail(format("Kirka liveness returned HTTP {code}, expected 200"))
        Logger.info("Kirka liveness OK")


if __name__ == "__main__":
    KirkaServiceCheck().execute()
