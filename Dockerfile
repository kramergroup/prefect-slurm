FROM prefecthq/prefect:3.7.0-python3.14

WORKDIR /app

COPY . /src/
RUN python -m pip install --no-cache-dir /src

RUN useradd -m prefect
USER prefect
