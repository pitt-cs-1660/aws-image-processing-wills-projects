FROM public.ecr.aws/lambda/python:3.13

# install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/


COPY pyproject.toml .
RUN uv sync
COPY handler.py ${LAMBDA_TASK_ROOT}/
ENV PYTHONPATH="${LAMBDA_TASK_ROOT}"
CMD ["handler.lambda_handler"]
