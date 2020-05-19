FROM amancevice/pandas:alpine

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY . /usr/src/app
RUN cd whl
RUN pip3 install numpy-1.18.4-cp36-cp36m-manylinux1_x86_64.whl
RUN pip3 install scipy-1.4.1-cp36-cp36m-manylinux1_x86_64
RUN cd ..

RUN pip3 install --no-cache-dir -r requirements.txt
ENV PYTHONUNBUFFERED 1
EXPOSE 5000

CMD ["gunicorn", "--workers=2", "--bind=0.0.0.0:8010", "app:app", "-t=600"]
