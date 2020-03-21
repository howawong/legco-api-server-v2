### 1. Get Linux
FROM python:3.6.4
RUN mkdir -p app
WORKDIR /app
COPY . /app
RUN pip install -r requirements.txt
ENV ENDPOINT=$ENDPOINT
ENV ADMIN_SECRET=$ADMIN_SECRET
ENV SLACK_TOKEN=$SLACK_TOKEN
ENV SLACK_CHANNEL=$SLACK_CHANNEL
CMD ["python", "/app/run.py"]
