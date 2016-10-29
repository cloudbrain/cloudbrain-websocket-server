FROM python:2.7.12

RUN apt-get update && apt-get install -y \
        git

RUN pip install scipy numpy --user

WORKDIR /
RUN git clone https://github.com/cloudbrain/cloudbrain.git
WORKDIR /cloudbrain
RUN python setup.py install --user

ADD . /app
WORKDIR /app
RUN python setup.py install --user

EXPOSE 31415
CMD ["python", "cbws/run.py"]
