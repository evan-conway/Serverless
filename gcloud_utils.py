import subprocess
import os
import sys
from gpu_vm import GPUVirtualMachine

def login_and_discover_vms(project_id):
    """
    Discovers VMs with GPUs in a given project.

    Args:
        project_id: The Google Cloud project ID.

    Returns:
        A list of GPUVirtualMachine objects.
    """

    print("\n--- Startup ---")
    print(f"Discovering VMs with GPUs in project: {project_id}...")
    try:
        command = f"gcloud compute instances list --project={project_id} --filter='guestAccelerators.acceleratorType~nvidia' --format='value(networkInterfaces[0].accessConfigs[0].natIp,networkInterfaces[0].networkIP,name,zone)'"
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        vms = []
        for line in result.stdout.strip().split('\n'):
            if not line:
                continue
            parts = line.split()
            ip = None
            name = None
            zone = None
            if len(parts) == 4:
                ip = parts[0]
                name = parts[2]
                zone = parts[3]
            elif len(parts) == 3:
                ip = parts[0]
                name = parts[1]
                zone = parts[2]
            
            if ip and name and zone:
                vms.append(GPUVirtualMachine(name, ip, zone, project_id))
        
        if not vms:
            print("No VMs with GPUs found.")
        else:
            print(f"Found {len(vms)} VMs with GPUs.")
            
        return vms
    except subprocess.CalledProcessError as e:
        print(f"Error discovering VMs: {e}", file=sys.stderr)
        print(f"Stderr: {e.stderr}", file=sys.stderr)
        return []
