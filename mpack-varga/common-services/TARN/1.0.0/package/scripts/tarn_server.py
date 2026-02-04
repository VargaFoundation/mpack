import sys
import os
from resource_management import *

class TarnServer(Script):
  def install(self, env):
    import params
    env.set_params(params)
    Logger.info("Installing Tarn Server")
    Directory([params.tarn_install_dir, params.tarn_conf_dir, params.tarn_log_dir, params.tarn_pid_dir],
              create_parents = True,
              mode=0755
    )
    # Copy JAR from mpack package
    File(os.path.join(params.tarn_install_dir, "tarn.jar"),
         content=StaticFile("tarn.jar"),
         owner='root',
         group='hadoop',
         mode=0644
    )

  def configure(self, env):
    import params
    env.set_params(params)

  def start(self, env):
    import params
    env.set_params(params)
    self.configure(env)
    Logger.info("Starting Tarn YARN Application")
    
    # Build yarn jar command
    cmd = format("yarn jar {tarn_install_dir}/tarn.jar varga.tarn.yarn.Client")
    cmd += format(" --model-repository {tarn_model_repository}")
    cmd += format(" --image {tarn_image}")
    cmd += format(" --port {tarn_port}")
    cmd += format(" --grpc-port {tarn_grpc_port}")
    cmd += format(" --metrics-port {tarn_metrics_port}")
    cmd += format(" --am-port {tarn_am_port}")
    cmd += format(" --address {tarn_bind_address}")
    
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

    # Async launch to avoid blocking Ambari
    # Note: Redirect output to capture application ID if needed, 
    # but YARN manages its own lifecycle.
    Execute(format("nohup {cmd} > {tarn_log_dir}/tarn_submit.out 2>&1 &"),
            user='root',
            not_if=format("yarn application -list -appTags TARN | grep RUNNING")
    )

  def stop(self, env):
    import params
    env.set_params(params)
    Logger.info("Stopping Tarn YARN Application")
    # Kill all applications with tag TARN
    Execute("yarn application -list -appTags TARN | grep application_ | awk '{print $1}' | xargs -I {} yarn application -kill {}",
            user='root',
            only_if="yarn application -list -appTags TARN | grep RUNNING"
    )

  def status(self, env):
    import params
    env.set_params(params)
    # Check if at least one application with tag TARN is running
    try:
        Execute("yarn application -list -appTags TARN | grep RUNNING",
                user='root'
        )
    except:
        raise ComponentIsNotRunning()

if __name__ == "__main__":
  TarnServer().execute()
