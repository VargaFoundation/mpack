import sys
import os
from resource_management import *

class KirkaServer(Script):
  def install(self, env):
    import params
    env.set_params(params)
    Logger.info("Installing Kirka Server")
    Directory([params.kirka_install_dir, params.kirka_conf_dir, params.kirka_log_dir, params.kirka_pid_dir],
              create_parents = True,
              mode=0755
    )
    # Copy JAR from mpack package
    File(os.path.join(params.kirka_install_dir, "kirka.jar"),
         content=StaticFile("kirka.jar"),
         owner='root',
         group='hadoop',
         mode=0644
    )

  def configure(self, env):
    import params
    env.set_params(params)
    File(format("{kirka_conf_dir}/application.properties"),
         content=Template("application.properties.j2"),
         mode=0644
    )
    if params.security_kerberos_enabled:
      # Ensure keytab exists and has correct permissions
      # In Ambari, this is usually handled by the kerberos.json mapping, 
      # but we can enforce it here if needed.
      pass

  def start(self, env):
    import params
    env.set_params(params)
    self.configure(env)
    Logger.info("Starting Kirka Server")
    process_cmd = format("{java_home}/bin/java -jar {kirka_install_dir}/kirka.jar --spring.config.location={kirka_conf_dir}/application.properties")
    Execute(format("nohup {process_cmd} > {kirka_log_dir}/kirka.out 2>&1 & echo $! > {kirka_pid_file}"),
            user='root',
            not_if=format("ls {kirka_pid_file} >/dev/null 2>&1 && ps -p `cat {kirka_pid_file}` >/dev/null 2>&1")
    )

  def stop(self, env):
    import params
    env.set_params(params)
    Logger.info("Stopping Kirka Server")
    Execute(format("kill `cat {kirka_pid_file}`"),
            only_if=format("test -f {kirka_pid_file} && ps -p `cat {kirka_pid_file}` >/dev/null 2>&1")
    )
    File(params.kirka_pid_file, action="delete")

  def status(self, env):
    import params
    env.set_params(params)
    check_process_status(params.kirka_pid_file)

if __name__ == "__main__":
  KirkaServer().execute()
