FROM python:3.12-slim
WORKDIR /app
COPY benchmark/analysis/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY scripts/algorithms.py benchmark/analysis/plot.py ./
ENTRYPOINT ["python", "plot.py"]
CMD ["results"]
