FROM prefecthq/prefect:3.1.12-python3.11
COPY requirements.txt /opt/prefect/prefect-slurm/requirements.txt
RUN python -m pip install -r /opt/prefect/prefect-slurm/requirements.txt
COPY . /opt/prefect/prefect-slurm/
WORKDIR /opt/prefect/prefect-slurm/
