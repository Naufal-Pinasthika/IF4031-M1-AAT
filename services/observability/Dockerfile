FROM python:3.12-alpine
WORKDIR /app
COPY server.py index.html ./
USER 1000:1000
EXPOSE 8090
CMD ["python", "-u", "server.py"]
