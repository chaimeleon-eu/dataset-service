FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip curl  && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
#bak: linux-libc-dev

ARG MAIN_DIR="/home/ubuntu"
WORKDIR ${MAIN_DIR}

# Install kubectl command
RUN curl -L "https://dl.k8s.io/release/v1.29.6/bin/linux/amd64/kubectl" -o kubectl \
 && chmod +x kubectl \
 && mv kubectl /usr/local/bin/
# && install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl


# Install requirements with apt
COPY requirements-apt.txt ${MAIN_DIR}/
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl $(cat requirements-apt.txt)  && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install requirements with pip
COPY requirements-pip.txt ${MAIN_DIR}/
#RUN pip install --no-cache-dir --upgrade pip \
RUN pip install --no-cache-dir --break-system-packages -r requirements-pip.txt

## Uncomment if you want to add the scripts dir to the image
## instead of mount it as a volume (see the job template)
#COPY scripts ${MAIN_DIR}

# CMD [ "/home/ubuntu/run.sh" ]
