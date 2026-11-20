FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY normaliser.py tracing.py test_normaliser.py ./
RUN python -m unittest -q test_normaliser
ENV PYTHONUNBUFFERED=1
CMD ["python", "normaliser.py"]
