import asyncio
import sys
import os
import time
import argparse
import json
from gcloud_utils import login_and_discover_vms
from scheduler import Scheduler

async def main():
    parser = argparse.ArgumentParser(description="Run and monitor GPU VM tasks from a JSON configuration file.")
    parser.add_argument("config_file", help="Path to the JSON configuration file")
    args = parser.parse_args()

    with open(args.config_file, 'r') as f:
        config = json.load(f)

    project_id = config.get("project_id")
    script_path = config.get("script_path")
    timeout = config.get("timeout")

    if not all([project_id, script_path, timeout]):
        print("Error: config file must contain 'project_id', 'script_path', and 'timeout'.")
        sys.exit(1)

    vms = login_and_discover_vms(project_id)

    if not vms:
        return

    scheduler = Scheduler(vms)
    
    # Add the initial job
    await scheduler.add_job(script_path, timeout)

    # Start polling and scheduling tasks
    polling_tasks = [asyncio.create_task(vm.poll_utilization()) for vm in vms]
    scheduler_task = asyncio.create_task(scheduler.schedule_loop())

    while True:
        print("\n--- Status ---")
        for vm in vms:
            status = "Idle" if vm.is_idle else "Active"
            running_script = vm.running_script if vm.running_script else "N/A"
            current_time = time.strftime('%Y-%m-%d %H:%M:%S')
            print(f"{current_time}: {status} | Running Script: {running_script}")
        
        jobs = await scheduler.get_jobs()
        if not jobs:
            print("No jobs in queue.")
        else:
            for job in jobs:
                print(f"{os.path.basename(job.script_path)} (Timeout: {job.timeout}s)")

        # Termination condition
        if not jobs and all(vm.running_script is None and not vm.has_results for vm in vms):
            print("\nAll jobs completed.")
            break

        await asyncio.sleep(5)

    # Cancel background tasks
    for task in polling_tasks:
        task.cancel()
    scheduler_task.cancel()
    await asyncio.gather(*polling_tasks, scheduler_task, return_exceptions=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExited.")
