"""Ambari script for the KIRKA_SERVER component.

Lifecycle: install -> configure -> start -> status -> stop. service_check is invoked by
Ambari after every restart to make sure the server is actually accepting traffic.

Logs land in /var/log/kirka, the PID file in /var/run/kirka. The JVM is launched with
`spring.profiles.active=prod` so the logback-spring.xml ships JSON to stdout — Ambari
captures stdout into the .out file in the log dir.
"""

import os
import time

try:
    # Python 3 path on newer Ambari agents
    from urllib.request import urlopen
    from urllib.error import URLError
except ImportError:  # pragma: no cover  (Ambari 2.7 still ships Python 2 in places)
    from urllib2 import urlopen, URLError

from resource_management import *


class KirkaServer(Script):

    # ------------------------------------------------------------------ install / configure

    def install(self, env):
        import params
        env.set_params(params)
        Logger.info("Installing Kirka Server")

        Directory(
            [params.kirka_install_dir, params.kirka_conf_dir,
             params.kirka_log_dir, params.kirka_pid_dir, params.kirka_data_dir,
             params.ranger_policy_cache_dir],
            create_parents=True,
            mode=0o755,
        )

        jar_target = os.path.join(params.kirka_install_dir, "kirka.jar")
        File(
            jar_target,
            content=StaticFile("kirka.jar"),
            owner='root',
            group='hadoop',
            mode=0o644,
        )

    def configure(self, env):
        import params
        env.set_params(params)

        # Re-create directories on every reconfigure so an admin who deleted /var/log/kirka
        # by mistake doesn't have to reinstall the service.
        Directory(
            [params.kirka_conf_dir, params.kirka_log_dir,
             params.kirka_pid_dir, params.kirka_data_dir,
             params.ranger_policy_cache_dir],
            create_parents=True,
            mode=0o755,
        )

        File(
            format("{kirka_conf_dir}/application.properties"),
            content=Template("application.properties.j2"),
            mode=0o640,
        )

    # ----------------------------------------------------------------------- start / stop

    def start(self, env):
        import params
        env.set_params(params)
        self.configure(env)

        jar_path = os.path.join(params.kirka_install_dir, "kirka.jar")
        if not os.path.isfile(jar_path):
            raise Fail(format("kirka.jar is missing at {jar_path} — reinstall the service "
                              "or rebuild the mpack with the latest Kirka JAR included."))

        # `Xmx` from kirka_heap_size; everything else from kirka_java_opts.
        #
        # We deliberately use `-cp HADOOP_CONF:HBASE_CONF:kirka.jar JarLauncher` instead of
        # `-jar kirka.jar`:
        #   - HDFS clients need `core-site.xml`/`hdfs-site.xml` on the classpath to resolve
        #     HA nameservices (e.g. `hdfs://clemlabtest`) into actual namenode addresses.
        #   - HBase clients need `hbase-site.xml` on the classpath to read the real
        #     `zookeeper.znode.parent` (typically `/hbase-secure` on Kerberized clusters,
        #     not the `/hbase` default).
        #   - `-jar` silently ignores `-cp`, so it cannot work for a Hadoop client.
        #
        # The launcher class moved in Spring Boot 3.2 from `org.springframework.boot.loader`
        # to `org.springframework.boot.loader.launch`. We pin the new package since the
        # whole project is on Spring Boot 3.2.x.
        hadoop_conf_dir = "/etc/hadoop/conf"
        hbase_conf_dir = "/etc/hbase/conf"
        process_cmd = format(
            "{java_home}/bin/java "
            "-Xmx{kirka_heap_size} {kirka_java_opts} "
            "-Dspring.profiles.active={kirka_spring_profiles_active} "
            "-cp {hadoop_conf_dir}:{hbase_conf_dir}:{kirka_install_dir}/kirka.jar "
            "org.springframework.boot.loader.launch.JarLauncher "
            "--spring.config.location=file:{kirka_conf_dir}/application.properties"
        )
        daemon_cmd = format(
            "nohup {process_cmd} >> {kirka_log_dir}/kirka.out 2>&1 & echo $! > {kirka_pid_file}"
        )

        Logger.info(format("Starting Kirka Server: {process_cmd}"))
        Execute(
            daemon_cmd,
            user='root',
            environment={
                'HADOOP_CONF_DIR': hadoop_conf_dir,
                'HBASE_CONF_DIR': hbase_conf_dir,
            },
            not_if=format(
                "test -f {kirka_pid_file} && ps -p `cat {kirka_pid_file}` >/dev/null 2>&1"
            ),
            logoutput=True,
        )

        # Block briefly so Ambari surfaces a startup failure synchronously instead of
        # reporting "started" and then immediately flipping to "stopped". HBase mini-cluster
        # / HA HDFS resolution can take noticeably longer than a vanilla Spring Boot app to
        # come up, so 120 s is the sweet spot in practice.
        self._wait_until_listening(params, timeout_seconds=120)

    def stop(self, env):
        import params
        env.set_params(params)

        Execute(
            format("kill `cat {kirka_pid_file}`"),
            user='root',
            only_if=format(
                "test -f {kirka_pid_file} && ps -p `cat {kirka_pid_file}` >/dev/null 2>&1"
            ),
            logoutput=True,
        )
        # Wait up to 35s (matches the 30s graceful + 5s slack) before forcing
        for _ in range(35):
            if not self._is_running(params.kirka_pid_file):
                break
            time.sleep(1)
        else:
            Logger.warning("Kirka did not exit within 35s; sending SIGKILL")
            Execute(
                format("kill -9 `cat {kirka_pid_file}` || true"),
                user='root',
                only_if=format("test -f {kirka_pid_file}"),
            )

        File(params.kirka_pid_file, action="delete")

    def status(self, env):
        import params
        env.set_params(params)
        check_process_status(params.kirka_pid_file)

    # ------------------------------------------------------------------- service check

    def service_check(self, env):
        """Called by Ambari after start (and on demand). Hits /actuator/health/liveness
        and fails the check if the response is anything other than 200."""
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

    # -------------------------------------------------------------------------- helpers

    @staticmethod
    def _is_running(pid_file):
        try:
            with open(pid_file) as f:
                pid = int(f.read().strip())
        except (IOError, ValueError):
            return False
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def _wait_until_listening(self, params, timeout_seconds=60):
        url = "http://localhost:{0}/actuator/health/liveness".format(params.kirka_port)
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            try:
                response = urlopen(url, timeout=5)
                if response.getcode() == 200:
                    Logger.info("Kirka started — liveness probe responded 200")
                    return
            except Exception:
                pass
            time.sleep(2)
        raise Fail(format(
            "Kirka did not respond on /actuator/health/liveness within {timeout_seconds}s. "
            "Inspect {kirka_log_dir}/kirka.out for stack traces."
        ))


if __name__ == "__main__":
    KirkaServer().execute()
