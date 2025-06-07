FROM python:3.9-slim

WORKDIR /app
COPY . .
RUN pip install flask grpcio grpcio-tools
EXPOSE 5001 50051 5002 50052
CMD ["python", "secondary.py"]