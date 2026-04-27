"""Configuration binding for the Kirka Ambari Mpack.

Translates entries from `kirka-site` into Python variables that are then injected into the
Jinja template for `application.properties`. Every lookup goes through `default()` so the
mpack tolerates upgrades from older versions: properties added in a later mpack release
that have not yet landed in Ambari's config DB fall back to a sane default instead of
crashing the template render."""

from resource_management import *
from resource_management.libraries.functions.default import default


def _cfg(key, fallback):
    """Read kirka-site/<key> with a documented fallback. Centralised so adding a new
    property in kirka-site.xml is a one-liner here."""
    return default(format('configurations/kirka-site/{key}'), fallback)


# --- Server -----------------------------------------------------------------------------
kirka_port = _cfg('kirka.port', '8086')
kirka_install_dir = _cfg('kirka.install_dir', '/opt/kirka')

# --- HBase / HDFS -----------------------------------------------------------------------
hbase_zookeeper_quorum = _cfg('hbase.zookeeper.quorum', 'localhost')
hbase_zookeeper_clientPort = _cfg('hbase.zookeeper.property.clientPort', '2181')
hadoop_hdfs_uri = _cfg('hadoop.hdfs.uri', 'hdfs://localhost:9000')

# --- API wire format --------------------------------------------------------------------
kirka_api_naming = _cfg('kirka.api.naming', 'snake_case')

# --- Security ---------------------------------------------------------------------------
security_enabled = _cfg('security.enabled', 'false')
security_authentication_type = _cfg('security.authentication.type', 'basic')
security_users_file = _cfg('security.users.file', '/etc/kirka/users.htpasswd')
security_trusted_proxies = _cfg('security.trusted.proxies', '')
security_admin_users = _cfg('security.admin.users', '')
security_authorization_owner_enabled = _cfg('security.authorization.owner.enabled', 'true')

# --- Kerberos ---------------------------------------------------------------------------
security_kerberos_enabled = _cfg('security.kerberos.enabled', 'false')
security_kerberos_principal = _cfg('security.kerberos.principal', 'kirka/_HOST@EXAMPLE.COM')
security_kerberos_keytab = _cfg('security.kerberos.keytab', '/etc/security/keytabs/kirka.service.keytab')
security_kerberos_krb5conf = _cfg('security.kerberos.krb5conf', '/etc/krb5.conf')

# --- Ranger -----------------------------------------------------------------------------
ranger_service_name = _cfg('ranger.service.name', 'kirka')
ranger_admin_url = _cfg('ranger.admin.url', 'http://localhost:6080')
ranger_policy_cache_dir = _cfg('ranger.policy.cache.dir', '/var/lib/kirka/ranger-cache')

# --- Lifecycle / multipart --------------------------------------------------------------
server_shutdown_timeout = _cfg('server.shutdown.timeout', '30s')
multipart_max_file_size = _cfg('multipart.max_file_size', '5GB')
multipart_max_request_size = _cfg('multipart.max_request_size', '5GB')

# --- JVM --------------------------------------------------------------------------------
kirka_heap_size = _cfg('kirka.heap.size', '2g')
kirka_java_opts = _cfg('kirka.java.opts', '-XX:+UseG1GC -XX:+ExitOnOutOfMemoryError')
kirka_spring_profiles_active = _cfg('kirka.spring.profiles.active', 'prod')

# --- Logging ----------------------------------------------------------------------------
logging_level_kirka = _cfg('logging.level.kirka', 'INFO')
logging_level_kirka_security = _cfg('logging.level.kirka.security', 'INFO')

# --- Filesystem layout (derived) --------------------------------------------------------
kirka_log_dir = "/var/log/kirka"
kirka_pid_dir = "/var/run/kirka"
kirka_pid_file = format("{kirka_pid_dir}/kirka.pid")
kirka_conf_dir = format("{kirka_install_dir}/conf")
kirka_data_dir = "/var/lib/kirka"
java_home = default('ambariLevelParams/java_home', '/usr/lib/jvm/java')
