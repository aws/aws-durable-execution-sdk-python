FROM public.ecr.aws/docker/library/python:3.13-alpine
WORKDIR /app
# scripts/ensure_microvm_image.py puts the worker wheel, built from this
# repo, in wheels/. pip installs boto3 from PyPI, at the versions that
# constraints.txt pins.
COPY constraints.txt wheels/ /tmp/build/
RUN pip install --no-cache-dir --root-user-action=ignore \
        --constraint /tmp/build/constraints.txt /tmp/build/*.whl \
    && rm -rf /tmp/build
COPY app.py .
EXPOSE 8080
# Lambda snapshots the running process after the ready hook.
CMD ["python", "-u", "app.py"]
