FROM python:3.10
COPY app/ ./app
COPY requirements.txt ./app/requirements.txt
WORKDIR .
RUN pip install --upgrade pip
RUN pip install -r app/requirements.txt
RUN python3 -m venv .venv
ENTRYPOINT python3 app/main.py
EXPOSE 5000
