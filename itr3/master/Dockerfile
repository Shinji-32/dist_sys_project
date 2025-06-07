FROM python:3.9-slim

WORKDIR /app
COPY . .
RUN pip install flask grpcio grpcio-tools
EXPOSE 5000 50050
CMD ["python", "master.py"]