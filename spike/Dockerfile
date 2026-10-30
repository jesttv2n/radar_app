FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends build-essential libexpat1 && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir numpy cython && \
    pip install --no-cache-dir pysteps rasterio h5py pyproj matplotlib opencv-python-headless requests

WORKDIR /spike
