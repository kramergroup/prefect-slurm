FROM prefecthq/prefect:3.1.12-python3.11

COPY . /src/
# RUN python -m pip install -r /src/requirements.txt
RUN python -m pip install /src

WORKDIR /

