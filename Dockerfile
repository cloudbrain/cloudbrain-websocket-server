FROM python:2.7.12

RUN apt-get update && apt-get install -y git

WORKDIR /
RUN git clone https://github.com/cloudbrain/cloudbrain.git
WORKDIR /cloudbrain
RUN pip install . --user

ADD . /app
WORKDIR /app
RUN pip install . --user

# Don't specify a path to JSON conf file. Use environment variables instead.
# Env variables to set:
# - AUTH_URL
# - PORT
# - RABBITMQ_ADDRESS
# - RABBITMQ_USER
# - RABBITMQ_PWD
EXPOSE 31415
CMD ["python", "-m", "cbws.run"]
