from resource_management import *
from resource_management.libraries.functions.default import default

config = Script.get_config()

kirka_port = config['configurations']['kirka-site']['kirka.port']
kirka_install_dir = config['configurations']['kirka-site']['kirka.install_dir']
hbase_zookeeper_quorum = config['configurations']['kirka-site']['hbase.zookeeper.quorum']
hbase_zookeeper_clientPort = config['configurations']['kirka-site']['hbase.zookeeper.property.clientPort']
hadoop_hdfs_uri = config['configurations']['kirka-site']['hadoop.hdfs.uri']
security_enabled = config['configurations']['kirka-site']['security.enabled']
security_authentication_type = config['configurations']['kirka-site']['security.authentication.type']
security_admin_users = config['configurations']['kirka-site']['security.admin.users']
security_kerberos_enabled = config['configurations']['kirka-site']['security.kerberos.enabled']
security_kerberos_principal = config['configurations']['kirka-site']['security.kerberos.principal']
security_kerberos_keytab = config['configurations']['kirka-site']['security.kerberos.keytab']
security_kerberos_krb5conf = config['configurations']['kirka-site']['security.kerberos.krb5conf']
ranger_service_name = config['configurations']['kirka-site']['ranger.service.name']
ranger_admin_url = config['configurations']['kirka-site']['ranger.admin.url']
ranger_policy_cache_dir = config['configurations']['kirka-site']['ranger.policy.cache.dir']
security_authorization_owner_enabled = config['configurations']['kirka-site']['security.authorization.owner.enabled']
logging_level_kirka = config['configurations']['kirka-site']['logging.level.kirka']

kirka_log_dir = "/var/log/kirka"
kirka_pid_dir = "/var/run/kirka"
kirka_pid_file = format("{kirka_pid_dir}/kirka.pid")
kirka_conf_dir = format("{kirka_install_dir}/conf")
java_home = config['ambariLevelParams']['java_home']
