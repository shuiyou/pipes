FROM amancevice/pandas:alpine

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY . /usr/src/app
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get autoremove -y \
    && apt-get install -y \
        gcc \
        build-essential \
        zlib1g-dev \
        wget \
        unzip \
        cmake \
        python3-dev \
        gfortran \
        libblas-dev \
        liblapack-dev \
        libatlas-base-dev \
    && apt-get clean

RUN pip3 install --no-cache-dir -r requirements.txt
ENV PYTHONUNBUFFERED 1
EXPOSE 5000

CMD ["gunicorn", "--workers=2", "--bind=0.0.0.0:8010", "app:app", "-t=600"]
