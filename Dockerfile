FROM ubuntu:22.04
LABEL name="dataset-service-backend"
LABEL description="https://github.com/chaimeleon-eu/dataset-service"
LABEL maintainer="palollo@i3m.upv.es"

RUN apt-get update && \
    apt-get install --no-install-recommends -y python3 python3-pip acl wget unzip less && \
    apt autoclean -y && \
    apt autoremove -y && \
    rm -rf /var/lib/apt/lists/*
# acl package provides the commands getfacl and setfacl to manage ACL on datalake files

ARG MAIN_DIR="/dataset-service"

RUN mkdir ${MAIN_DIR} ${MAIN_DIR}/etc ${MAIN_DIR}/log
COPY start_dataset_service.py requirements.txt API-reference-v1.yaml README.md LICENSE ${MAIN_DIR}/
COPY ./dataset_service/ ${MAIN_DIR}/dataset_service
COPY ./etc/dataset-service.default.yaml ${MAIN_DIR}/etc/
#COPY setup.py ${MAIN_DIR}/setup.py

# RUN pip install setuptools 
# RUN cd ${MAIN_DIR} && python3 setup.py install 
# CMD start_dataset_service.py 

WORKDIR ${MAIN_DIR}
RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir -r requirements.txt 

#CMD python3 start_dataset_service.py && tail -f /dataset-service/log/dataset-service.log
#CMD python3 start_dataset_service.py
# With this ENTRYPOINT the python process can receive the SIGTERM to do a graceful termination of the service,
# The default ENTRYPOINT ["/bin/sh", "-c"] does not propagate the signals.
ENTRYPOINT ["/usr/bin/python3", "start_dataset_service.py"]

EXPOSE 11000
