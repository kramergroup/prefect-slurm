from prefect_slurm.api.jobs import APIEndpoint, JobDefinition

jobdef = JobDefinition(
    partition="small",
    time_limit="00:10:00",
    tasks=1,
    current_working_directory="/beegfs/home/k/kramerd",
)

jobdef2 = JobDefinition.from_kwargs(
    {"p": "small", "t": "01:00:00", "n": 2, "chdir": "$home"}
)
print(jobdef2)

api = APIEndpoint(
    endpoint="https://hsuper-login.hsu-hh.de/slurm/v0.0.38/",
    username="kramerd",
    token="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE2OTQ0Mjg2MDQsImlhdCI6MTY5NDQyMjYwNCwic3VuIjoia3JhbWVyZCJ9.Ru6D5ZH7MPnXI3ZQ2dVIdq3jKHtvr3W81_PcIzLRxsk",  # noqa: E501
)

# async def main():
#     task_submit = api.submit(jobdef)
#     res = await asyncio.wait_for(task_submit, timeout=10)
#     print(res)
#     job_id = res.job_id
#     task_status = api.status(job_id)
#     res = await asyncio.wait_for(task_status, timeout=10)
#     print(res)
#     task_cancel = api.cancel(job_id)
#     res = await asyncio.wait_for(task_cancel, timeout=10)
#     print(res)
#     task_status = api.status(job_id)
#     res = await asyncio.wait_for(task_status, timeout=10)
#     print(res)

# asyncio.run(main())
