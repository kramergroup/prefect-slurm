FROM python:3.14.4-slim

COPY ./requirements.txt /src/requirements.txt
COPY flow.py /src/

# Singularity does not respect WORKDIR and executes in a mounted home directory
# by default. EntrypointType.MODULE_PATH must also be set in deploy.
ENV PYTHONPATH=/src:${PYTHONPATH}

RUN pip install --no-cache-dir -r /src/requirements.txt

ARG DEBUG=1
ENV PREFECT_DEBUG_MODE=$DEBUG

CMD [ "prefect", "flow-run", "execute" ]
