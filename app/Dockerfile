FROM python:3.10
COPY app/ ./app
WORKDIR .
RUN pip install --upgrade pip
RUN pip install -r app/requirements.txt
RUN python3 -m venv .venv
ENTRYPOINT python3 app/app.py
EXPOSE 5000