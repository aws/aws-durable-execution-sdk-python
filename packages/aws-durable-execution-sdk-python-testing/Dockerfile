FROM python:3.13-slim

# Copy and install the wheel
COPY dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm -rf /tmp/*.whl

# AWS credentials (required for boto3)
ENV AWS_ACCESS_KEY_ID=foo \
    AWS_SECRET_ACCESS_KEY=bar \
    AWS_DEFAULT_REGION=us-east-1

EXPOSE 9014

CMD ["dex-local-runner", "start-server", \
     "--host", "0.0.0.0", \
     "--port", "9014", \
     "--log-level", "DEBUG", \
     "--lambda-endpoint", "http://host.docker.internal:3001", \
     "--store-type", "sqlite", \
     "--store-path", "/tmp/.durable-executions-local/durable-executions.db"]
