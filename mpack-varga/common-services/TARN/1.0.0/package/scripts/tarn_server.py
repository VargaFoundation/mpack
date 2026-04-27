import sys
import os
import json
import urllib.request
import urllib.error
import ssl
import base64
from resource_management import *

class TarnServer(Script):
  def install(self, env):
    import params
    env.set_params(params)
    Logger.info("Installing Tarn Server")
    Directory([params.tarn_install_dir, params.tarn_conf_dir, params.tarn_log_dir, params.tarn_pid_dir],
              create_parents = True,
              mode=0o755
    )
    # Copy JAR from mpack package
    File(os.path.join(params.tarn_install_dir, "tarn.jar"),
         content=StaticFile("tarn.jar"),
         owner='root',
         group='hadoop',
         mode=0o644
    )
    self._ensure_ranger_yarn_policy(params)

  def _ranger_api(self, method, path, params, data=None):
    """Call the Ranger Admin REST API."""
    # Build Ranger base URL (prefer HTTPS)
    ranger_url = params.ranger_host.rstrip('/')
    if ranger_url.startswith('http://'):
      # Replace with HTTPS on standard port
      ranger_url = ranger_url.replace('http://', 'https://').rstrip('/')
      # Replace port 6080 with 6182 for HTTPS
      ranger_url = ranger_url.replace(':6080', ':' + params.ranger_https_port)
    url = ranger_url + path
    auth = base64.b64encode('{0}:{1}'.format(params.ranger_admin_username, params.ranger_admin_password).encode()).decode()
    headers = {
      'Content-Type': 'application/json',
      'Authorization': 'Basic ' + auth
    }
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, method=method, headers=headers)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    resp = urllib.request.urlopen(req, context=ctx, timeout=30)
    return json.loads(resp.read().decode()) if resp.status == 200 else None

  def _ensure_ranger_yarn_policy(self, params):
    """Create a Ranger policy allowing 'tarn' to submit YARN applications."""
    if not params.ranger_admin_password:
      Logger.warning("Ranger admin password not available, skipping Ranger YARN policy creation. "
                     "Manually add 'tarn' to YARN queue submit policy in Ranger.")
      return
    try:
      svc = params.ranger_yarn_service_name
      Logger.info("Checking Ranger YARN policies for service: {0}".format(svc))
      # Check if a policy for tarn already exists
      policies = self._ranger_api('GET', '/service/public/v2/api/service/{0}/policy'.format(svc), params)
      for policy in (policies or []):
        for item in policy.get('policyItems', []):
          if 'tarn' in item.get('users', []):
            Logger.info("Ranger YARN policy already contains 'tarn' user in policy: {0}".format(policy['name']))
            return
      # Create a new policy
      policy_data = {
        "policyType": 0,
        "name": "tarn-yarn-queue-access",
        "isEnabled": True,
        "isAuditEnabled": True,
        "resources": {"queue": {"values": ["root.default", "default"], "isRecursive": True}},
        "policyItems": [{
          "users": ["tarn"],
          "accesses": [
            {"type": "submit-app", "isAllowed": True},
            {"type": "admin-queue", "isAllowed": False}
          ],
          "delegateAdmin": False
        }],
        "service": svc
      }
      result = self._ranger_api('POST', '/service/plugins/policies', params, data=policy_data)
      if result and result.get('id'):
        Logger.info("Created Ranger YARN policy 'tarn-yarn-queue-access' (ID: {0})".format(result['id']))
      else:
        Logger.warning("Ranger policy creation returned unexpected result: {0}".format(result))
    except Exception as e:
      Logger.warning("Failed to create Ranger YARN policy: {0}. "
                     "Manually add 'tarn' user to YARN queue submit policy in Ranger Admin.".format(e))

  def configure(self, env):
    import params
    env.set_params(params)
    self._render_jaas(params)

  def _render_jaas(self, params):
    """Write a JAAS config that the AM JVM will use to authenticate to a Kerberized
    ZooKeeper. Without this, the AM hangs in CuratorFramework.blockUntilConnected because
    SASL handshakes never succeed.

    We use the smokeuser (ambari-qa) keytab because it is guaranteed to exist on every
    NodeManager host (LinuxContainerExecutor requires it) — the tarn service keytab is
    only on the master, so the AM container, which lands on a random NM, would not have
    it. The principal need not match a Ranger 'tarn' user; it only needs to be valid
    Kerberos credentials accepted by ZooKeeper's SASL provider.
    """
    if params.security_kerberos_enabled == 'true' or params.security_kerberos_enabled == True:
      jaas_path = os.path.join(params.tarn_conf_dir, "tarn_zk_jaas.conf")
      jaas_content = (
        "Client {{\n"
        "  com.sun.security.auth.module.Krb5LoginModule required\n"
        "  useKeyTab=true\n"
        "  keyTab=\"{keytab}\"\n"
        "  storeKey=true\n"
        "  useTicketCache=false\n"
        "  principal=\"{principal}\";\n"
        "}};\n"
      ).format(keytab=params.smokeuser_keytab,
               principal=params.smokeuser_principal)
      File(jaas_path,
           content=jaas_content,
           owner='root',
           group='hadoop',
           mode=0o644,
      )
      Logger.info("Wrote ZK JAAS config to {0}".format(jaas_path))

  def _kinit(self, params):
    """Authenticate with Kerberos using the smokeuser (ambari-qa) keytab.

    We use ambari-qa because it exists on all cluster hosts (required by
    YARN's LinuxContainerExecutor) and has YARN queue submit permissions.
    The tarn service principal may not exist on all NodeManager hosts.
    """
    if params.security_kerberos_enabled == 'true' or params.security_kerberos_enabled == True:
      Execute(format("kinit -kt {smokeuser_keytab} {smokeuser_principal}"),
              user='root',
              logoutput=True
      )

  def start(self, env):
    import params
    env.set_params(params)
    self.configure(env)
    Logger.info("Starting Tarn Client Daemon")
    self._kinit(params)

    # Build the yarn jar command with all parameters
    cmd = format("yarn jar {tarn_install_dir}/tarn.jar varga.tarn.yarn.Client")
    cmd += format(" --jar {tarn_install_dir}/tarn.jar")
    cmd += format(" --model-repository {tarn_model_repository}")
    cmd += format(" --image {tarn_image}")
    cmd += format(" --port {tarn_port}")
    cmd += format(" --grpc-port {tarn_grpc_port}")
    cmd += format(" --metrics-port {tarn_metrics_port}")
    cmd += format(" --am-port {tarn_am_port}")
    cmd += format(" --address {tarn_bind_address}")
    cmd += format(" --client-port {tarn_client_port}")

    if params.tarn_token:
        cmd += format(" --token {tarn_token}")

    cmd += format(" --tp {tarn_tensor_parallelism}")
    cmd += format(" --pp {tarn_pipeline_parallelism}")

    if params.tarn_secrets_path:
        cmd += format(" --secrets {tarn_secrets_path}")

    cmd += format(" --placement-tag {tarn_placement_tag}")
    cmd += format(" --docker-network {tarn_docker_network}")

    if params.tarn_docker_privileged == 'true' or params.tarn_docker_privileged == True:
        cmd += " --docker-privileged"
    if params.tarn_docker_delayed_removal == 'true' or params.tarn_docker_delayed_removal == True:
        cmd += " --docker-delayed-removal"
    if params.tarn_docker_mounts:
        cmd += format(" --docker-mounts {tarn_docker_mounts}",
                      tarn_docker_mounts=params.tarn_docker_mounts)

    if params.tarn_zk_ensemble:
        cmd += format(" --zk-ensemble {tarn_zk_ensemble}")
    cmd += format(" --zk-path {tarn_zk_path}")

    if params.tarn_ranger_service:
        cmd += format(" --ranger-service {tarn_ranger_service}")
    cmd += format(" --ranger-app-id {tarn_ranger_app_id}")
    if params.tarn_ranger_audit == 'true' or params.tarn_ranger_audit == True:
        cmd += " --ranger-audit"

    cmd += format(" --scale-up {tarn_scale_up_threshold}")
    cmd += format(" --scale-down {tarn_scale_down_threshold}")
    cmd += format(" --min-instances {tarn_min_instances}")
    cmd += format(" --max-instances {tarn_max_instances}")
    cmd += format(" --cooldown {tarn_cooldown}")

    # ===== v1.0.0 additions =====
    # Bool/string toggles. Cast each Ambari config value defensively so 'true', True, 'True'
    # all behave the same way (Ambari stringifies booleans inconsistently across versions).
    def _truthy(v):
      return str(v).strip().lower() in ('true', 'yes', '1')

    if _truthy(params.tarn_ranger_strict):
      cmd += " --ranger-strict"
    if _truthy(params.tarn_zk_required):
      cmd += " --zk-required"

    # TLS — only emit the flags if the operator wired a keystore path; the AM validates
    # tls-enabled requires --tls-keystore so we skip both together if either is missing.
    if _truthy(params.tarn_tls_enabled) and params.tarn_tls_keystore_path:
      cmd += format(" --tls-enabled --tls-keystore {tarn_tls_keystore_path}"
                    " --tls-keystore-type {tarn_tls_keystore_type}"
                    " --tls-keystore-password-alias {tarn_tls_keystore_pwd_alias}",
                    tarn_tls_keystore_path=params.tarn_tls_keystore_path,
                    tarn_tls_keystore_type=params.tarn_tls_keystore_type,
                    tarn_tls_keystore_pwd_alias=params.tarn_tls_keystore_pwd_alias)

    # OpenAI proxy
    if _truthy(params.tarn_openai_proxy_enabled):
      cmd += format(" --openai-proxy-enabled --openai-proxy-port {tarn_openai_proxy_port}",
                    tarn_openai_proxy_port=params.tarn_openai_proxy_port)

    # OTel endpoint (only meaningful if the JVM has the otel javaagent attached)
    if params.tarn_otel_endpoint:
      cmd += format(" --otel-endpoint {tarn_otel_endpoint}",
                    tarn_otel_endpoint=params.tarn_otel_endpoint)

    # Scaling additions
    cmd += format(" --scale-mode {tarn_scale_mode}", tarn_scale_mode=params.tarn_scale_mode)
    cmd += format(" --queue-capacity-per-container {tarn_queue_capacity}",
                  tarn_queue_capacity=params.tarn_queue_capacity)
    cmd += format(" --monitor-interval-ms {tarn_monitor_interval_ms}",
                  tarn_monitor_interval_ms=params.tarn_monitor_interval_ms)
    cmd += format(" --drain-timeout-ms {tarn_drain_timeout_ms}",
                  tarn_drain_timeout_ms=params.tarn_drain_timeout_ms)
    cmd += format(" --warmup-timeout-ms {tarn_warmup_timeout_ms}",
                  tarn_warmup_timeout_ms=params.tarn_warmup_timeout_ms)
    cmd += format(" --warmup-poll-interval-ms {tarn_warmup_poll_ms}",
                  tarn_warmup_poll_ms=params.tarn_warmup_poll_ms)

    # Quotas (HDFS path)
    if params.tarn_quotas_path:
      cmd += format(" --quotas {tarn_quotas_path}",
                    tarn_quotas_path=params.tarn_quotas_path)

    # Accelerator
    cmd += format(" --accelerator-type {tarn_accelerator_type}",
                  tarn_accelerator_type=params.tarn_accelerator_type)
    if params.tarn_gpu_slice_size:
      cmd += format(" --gpu-slice-size {tarn_gpu_slice_size}",
                    tarn_gpu_slice_size=params.tarn_gpu_slice_size)

    # Shadow traffic
    if params.tarn_shadow_endpoint and float(params.tarn_shadow_sample_rate or 0) > 0:
      cmd += format(" --shadow-endpoint {tarn_shadow_endpoint} --shadow-sample-rate {tarn_shadow_sample_rate}",
                    tarn_shadow_endpoint=params.tarn_shadow_endpoint,
                    tarn_shadow_sample_rate=params.tarn_shadow_sample_rate)

    # When Kerberos is enabled, pass the JAAS conf to the AM via --zk-jaas. Client.java
    # uploads it to HDFS as a LocalResource and sets JAVA_TOOL_OPTIONS on the AM JVM so the
    # JVM picks it up automatically — no need to mount /etc/tarn into NodeManager containers.
    if params.security_kerberos_enabled == 'true' or params.security_kerberos_enabled == True:
      jaas_path = os.path.join(params.tarn_conf_dir, "tarn_zk_jaas.conf")
      cmd += format(" --zk-jaas {jaas_path}")

    Logger.info(format("Launching Tarn Client daemon: {cmd}"))
    daemon_cmd = format("nohup bash -c 'export JAVA_HOME={java_home}; {cmd}' >> {tarn_log_dir}/tarn_client.out 2>&1 & echo $! > {tarn_pid_file}")
    Execute(daemon_cmd,
            user='root',
            not_if=format("ls {tarn_pid_file} >/dev/null 2>&1 && ps -p `cat {tarn_pid_file}` >/dev/null 2>&1"),
            logoutput=True
    )
    Logger.info(format("Tarn Client daemon started, PID file: {tarn_pid_file}"))

  def stop(self, env):
    import params
    env.set_params(params)
    Logger.info("Stopping Tarn Client Daemon")
    # Kill the Client process; its shutdown hook will kill the YARN application
    Execute(format("kill `cat {tarn_pid_file}`"),
            user='root',
            only_if=format("test -f {tarn_pid_file} && ps -p `cat {tarn_pid_file}` >/dev/null 2>&1"),
            logoutput=True
    )
    # Wait for process to terminate (shutdown hook needs time to kill YARN app)
    Execute(format("for i in $(seq 1 30); do ps -p `cat {tarn_pid_file} 2>/dev/null` >/dev/null 2>&1 || break; sleep 1; done"),
            user='root',
            logoutput=True
    )
    File(params.tarn_pid_file, action="delete")
    Logger.info("Tarn Client Daemon stopped")

  def status(self, env):
    import params
    env.set_params(params)
    check_process_status(params.tarn_pid_file)

if __name__ == "__main__":
  TarnServer().execute()
