FROM python:3.12

COPY ./requirements.txt /src/requirements.txt
COPY test_flow.py /src/test_flow.py

# This is needed because singularity does not respect the WORKDIR directive
# and executes in a mounted home directory by default.
# You also need to set the entrypoint_type to EntrypointType.MODULE_PATH in
# deploy, so prefect is looking for the test_flow function in the test_flow module
ENV PYTHONPATH=/src:${PYTHONPATH}

RUN pip install -r /src/requirements.txt

CMD [ "prefect", "flow-run", "execute" ]
