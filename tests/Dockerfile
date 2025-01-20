FROM python:3.12

COPY ./requirements.txt /requirements.txt
COPY test_flow.py .

RUN pip install -r /requirements.txt

ENTRYPOINT [ "prefect", "flow-run", "execute" ]