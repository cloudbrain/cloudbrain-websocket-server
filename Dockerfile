FROM python:2.7.12

ADD . /app
WORKDIR /app
RUN python setup.py install --user

EXPOSE 31415
CMD ["python", "cbws/run.py"]
