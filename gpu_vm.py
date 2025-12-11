import asyncio
import subprocess
import os
import collections
import time

DELAY = 1
UTILIZATION_CEILING = 20

class GPUVirtualMachine:
    """
    Represents a Google Cloud VM with a GPU.
    """
    def __init__(self, name, ip, zone, project_id):
        self.name = name
        self.ip = ip
        self.zone = zone
        self.project_id = project_id
        self.utilization_history = collections.deque([100] * 120, maxlen=120)
        self.other_utilization_history = collections.deque([100] * 120, maxlen=120)
        self.is_idle = False
        self.script_pid = None
        self.running_script = None
        self.running_job = None
        self.has_results = False
        self.last_job_output = None

    def _check_idle_status(self):
        """
        Checks if the GPU has been idle for the last minute based on other processes' utilization.
        """
        if len(self.other_utilization_history) < DELAY:
            self.is_idle = False
            return

        recent_history = list(self.other_utilization_history)[-DELAY:]
        self.is_idle = all(util < UTILIZATION_CEILING for util in recent_history)

    def update_utilization(self):
        """
        Checks GPU utilization on the remote VM via gcloud compute ssh.
        """
        try:
            # Check if the script is still running
            if self.script_pid:
                ps_command = f"gcloud compute ssh {self.name} --zone {self.zone} --project {self.project_id} --command 'ps -p {self.script_pid}'"
                ps_result = subprocess.run(ps_command, shell=True, check=False, capture_output=True, text=True)

                if ps_result.returncode != 0:
                    # Script has finished, retrieve results
                    log_file = f"{os.path.splitext(self.running_script)[0]}_{int(self.running_job.submission_time)}.log"
                    remote_log_path = f"/tmp/{log_file}"
                    local_log_path = f"./{log_file}"
                    
                    # Copy log file from remote
                    scp_command = f"gcloud compute scp {self.name}:{remote_log_path} {local_log_path} --zone {self.zone} --project {self.project_id}"
                    subprocess.run(scp_command, shell=True, check=True, capture_output=True, text=True)
                    
                    # Read log file
                    with open(local_log_path, 'r') as f:
                        self.last_job_output = f.read()
                    
                    # Clean up local log file
                    os.remove(local_log_path)

                    # Clean up remote log file
                    rm_command = f"gcloud compute ssh {self.name} --zone {self.zone} --project {self.project_id} --command 'rm {remote_log_path}'"
                    subprocess.run(rm_command, shell=True, check=False, capture_output=True, text=True)

                    self.has_results = True
                    self.script_pid = None
                    self.running_script = None
                    self.running_job = None

            # Get total utilization
            total_util_command = f"gcloud compute ssh {self.name} --zone {self.zone} --project {self.project_id} --command 'nvidia-smi --query-gpu=utilization.gpu --format=csv,noheader,nounits'"
            total_util_result = subprocess.run(total_util_command, shell=True, check=True, capture_output=True, text=True)
            total_utilization = int(total_util_result.stdout.strip())
            self.utilization_history.append(total_utilization)

            if self.script_pid:
                # get all process children
                try:
                    process_children_command = f"gcloud compute ssh {self.name} --zone {self.zone} --project {self.project_id} --command 'pgrep -P {self.script_pid}'"
                    process_children_result = subprocess.run(process_children_command, shell=True, check=True, capture_output=True, text=True)
                    process_children = [int(x) for x in process_children_result.stdout.strip().split("\n")]
                except:
                    process_children = []
                process_children.append(self.script_pid) # type: ignore

                # Get per-process utilization
                pmon_command = f"gcloud compute ssh {self.name} --zone {self.zone} --project {self.project_id} --command 'nvidia-smi pmon -c 1'"
                pmon_result = subprocess.run(pmon_command, shell=True, check=True, capture_output=True, text=True)
                
                script_utilization = 0
                if self.script_pid:
                    lines = pmon_result.stdout.strip().split('\n')
                    for line in lines:
                        if line.startswith('#'):
                            continue
                        parts = line.split()
                        try:
                            if len(parts) >= 4 and int(parts[1]) in process_children:
                                script_utilization += int(parts[3])
                                break
                        except:
                            pass
            else:
                script_utilization = 0
            
            other_utilization = total_utilization - script_utilization
            self.other_utilization_history.append(other_utilization)
            
            self._check_idle_status()

        except Exception as e:
            print(e)

    async def poll_utilization(self):
        """
        Asynchronously polls GPU utilization on the remote VM every 5 seconds.
        """
        while True:
            await asyncio.to_thread(self.update_utilization)
            await asyncio.sleep(5)

    async def execute_script(self, job):
        """
        Executes a Python script on the remote VM with a timeout and saves the output to a file.
        The script is executed in the background.

        Args:
            job: A Job object containing the script path and timeout.
        """
        try:
            script_path = job.script_path
            timeout = job.timeout
            
            # Copy the script to the remote VM
            script_name = os.path.basename(script_path)
            remote_path = f"/tmp/{script_name}"
            scp_command = f"gcloud compute scp {script_path} {self.name}:{remote_path} --zone {self.zone} --project {self.project_id}"
            await asyncio.to_thread(subprocess.run, scp_command, shell=True, check=True, capture_output=True, text=True)

            # Execute the script in the background and get its PID
            log_file = f"{os.path.splitext(script_name)[0]}_{int(job.submission_time)}.log"
            remote_log_path = f"/tmp/{log_file}"
            
            self.running_script = script_name
            self.running_job = job
            
            ssh_command = f"gcloud compute ssh {self.name} --zone {self.zone} --project {self.project_id} " f'--command "timeout {timeout} /opt/python/3.10/bin/python {remote_path} > {remote_log_path} 2>&1 & echo \\$!"'
            result = await asyncio.to_thread(subprocess.run, ssh_command, shell=True, check=True, capture_output=True, text=True)
            pid = int(result.stdout.strip())
            self.script_pid = pid

        except Exception as e:
            print(e)
            self.running_script = None
            self.script_pid = None
            self.running_job = None