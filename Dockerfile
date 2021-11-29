FROM ubuntu:20.04
LABEL name="dataset-service-backend"
LABEL description="dataset-service-backend"
LABEL version="0.18"
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
RUN pip install --upgrade pip && pip install -r requirements.txt 

CMD ./start_dataset_service.py 

EXPOSE 11000
