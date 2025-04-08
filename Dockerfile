FROM python:3.13-alpine

WORKDIR /workspace

ADD requirements.txt .
RUN pip install -r requirements.txt

ADD main.py .

ENTRYPOINT ["python", "main.py"]