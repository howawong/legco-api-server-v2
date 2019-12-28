FROM python:3.7-slim-stretch
COPY ./requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt
COPY *.py /app/
COPY jobs /app/jobs
RUN find /app
ENV FLASK_APP server.py
WORKDIR /app
CMD ["flask", "run", "-p", "8888", "-h", "0.0.0.0"]
