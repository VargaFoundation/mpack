from resource_management import *
from resource_management.libraries.functions.default import default

config = Script.get_config()

tarn_model_repository = config['configurations']['tarn-site']['tarn.model.repository']
tarn_image = config['configurations']['tarn-site']['tarn.image']
tarn_port = config['configurations']['tarn-site']['tarn.port']
tarn_grpc_port = config['configurations']['tarn-site']['tarn.grpc.port']
tarn_metrics_port = config['configurations']['tarn-site']['tarn.metrics.port']
tarn_am_port = config['configurations']['tarn-site']['tarn.am.port']
tarn_bind_address = config['configurations']['tarn-site']['tarn.bind.address']
tarn_token = config['configurations']['tarn-site']['tarn.token']
tarn_tensor_parallelism = config['configurations']['tarn-site']['tarn.tensor.parallelism']
tarn_pipeline_parallelism = config['configurations']['tarn-site']['tarn.pipeline.parallelism']
tarn_secrets_path = config['configurations']['tarn-site']['tarn.secrets.path']
tarn_placement_tag = config['configurations']['tarn-site']['tarn.placement.tag']
tarn_docker_network = config['configurations']['tarn-site']['tarn.docker.network']
tarn_docker_privileged = config['configurations']['tarn-site']['tarn.docker.privileged']
tarn_docker_delayed_removal = config['configurations']['tarn-site']['tarn.docker.delayed.removal']
tarn_zk_ensemble = config['configurations']['tarn-site']['tarn.zk.ensemble']
tarn_zk_path = config['configurations']['tarn-site']['tarn.zk.path']
tarn_ranger_service = config['configurations']['tarn-site']['tarn.ranger.service']
tarn_ranger_app_id = config['configurations']['tarn-site']['tarn.ranger.app.id']
tarn_ranger_audit = config['configurations']['tarn-site']['tarn.ranger.audit']
tarn_scale_up_threshold = config['configurations']['tarn-site']['tarn.scale.up.threshold']
tarn_scale_down_threshold = config['configurations']['tarn-site']['tarn.scale.down.threshold']
tarn_min_instances = config['configurations']['tarn-site']['tarn.min.instances']
tarn_max_instances = config['configurations']['tarn-site']['tarn.max.instances']
tarn_cooldown = config['configurations']['tarn-site']['tarn.cooldown']
tarn_client_port = config['configurations']['tarn-site']['tarn.client.port']
tarn_install_dir = config['configurations']['tarn-site']['tarn.install_dir']
security_kerberos_enabled = config['configurations']['tarn-site']['security.kerberos.enabled']
security_kerberos_principal = config['configurations']['tarn-site']['security.kerberos.principal']
security_kerberos_keytab = config['configurations']['tarn-site']['security.kerberos.keytab']
ranger_admin_url = config['configurations']['tarn-site']['ranger.admin.url']

# Ranger admin credentials (from ranger-env, decrypted by Ambari)
ranger_env = default('/configurations/ranger-env', {})
ranger_admin_username = ranger_env.get('ranger_admin_username', 'amb_ranger_admin') if ranger_env else 'admin'
ranger_admin_password = ranger_env.get('ranger_admin_password', '') if ranger_env else ''
# Ranger YARN service name follows Ambari convention: {cluster_name}_yarn
cluster_name = config['clusterName']
ranger_yarn_service_name = cluster_name + '_yarn'
# Ranger admin HTTPS URL
ranger_https_port = default('/configurations/ranger-admin-site/ranger.service.https.port', '6182')
ranger_http_enabled = default('/configurations/ranger-admin-site/ranger.service.http.enabled', 'false')
ranger_host = default('/configurations/ranger-admin-site/ranger.externalurl', ranger_admin_url)

# Smokeuser (ambari-qa) credentials for YARN submission
smokeuser_keytab = default('/configurations/cluster-env/smokeuser_keytab', '/etc/security/keytabs/smokeuser.headless.keytab')
smokeuser_principal = default('/configurations/cluster-env/smokeuser_principal_name', 'ambari-qa')

tarn_log_dir = "/var/log/tarn"
tarn_pid_dir = "/var/run/tarn"
tarn_pid_file = format("{tarn_pid_dir}/tarn.pid")
tarn_conf_dir = format("{tarn_install_dir}/conf")
java_home = config['ambariLevelParams']['java_home']
