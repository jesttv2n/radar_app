FROM python:3.9-bullseye

RUN apt-get update && \
    apt-get install -y tzdata && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --upgrade \
    h5py \
    numpy \
    opencv-python \
    Pillow \
    pytz \
    matplotlib \
    requests \
    schedule \
    boto3 \
    python-dotenv

ENV TZ="Europe/Copenhagen"

WORKDIR /app

COPY *.py /app/
COPY static/TV2.ttf /app/static/TV2.ttf

CMD ["python3", "/app/master.py"]
