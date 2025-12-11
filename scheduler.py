import asyncio
import time
import os

class Job:
    def __init__(self, script_path, timeout):
        self.script_path = script_path
        self.timeout = timeout
        self.submission_time = time.time()

    def __lt__(self, other):
        return self.submission_time < other.submission_time

class Scheduler:
    def __init__(self, vms):
        self.vms = vms
        self.job_queue = asyncio.PriorityQueue()

    async def add_job(self, script_path, timeout):
        job = Job(script_path, timeout)
        await self.job_queue.put(job)
        print(f"Added job for script: {script_path}")

    async def get_jobs(self):
        """
        Returns a list of jobs in the queue without modifying the queue.
        """
        jobs = []
        temp_queue = asyncio.PriorityQueue()
        while not self.job_queue.empty():
            job = await self.job_queue.get()
            jobs.append(job)
            await temp_queue.put(job)
        self.job_queue = temp_queue
        return jobs

    async def schedule_loop(self):
        while True:
            # Assign jobs to idle VMs
            for vm in self.vms:
                if vm.is_idle and vm.running_script is None:
                    if not self.job_queue.empty():
                        print("\n--- Scheduling ---")
                        print("Scheduler trying to get a job...")
                        job = await self.job_queue.get()
                        print(f"Scheduler got job: {job.script_path}")
                        print(f"Assigning job {job.script_path} to VM {vm.name}")
                        asyncio.create_task(vm.execute_script(job))

            # Poll for results
            for vm in self.vms:
                if vm.has_results:
                    print("\n--- Results ---")
                    print(vm.last_job_output)

                    vm.has_results = False
                    vm.last_job_output = None
            
            await asyncio.sleep(5)
