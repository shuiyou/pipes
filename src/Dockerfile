FROM amancevice/pandas:alpine

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY . /usr/src/app
RUN apk --update add --no-cache \
    lapack-dev \
    gcc \
    freetype-dev
RUN pip3 install --no-cache-dir -r requirements.txt
ENV PYTHONUNBUFFERED 1
EXPOSE 5000

CMD ["gunicorn", "--workers=2", "--bind=0.0.0.0:8010", "app:app", "-t=600"]
