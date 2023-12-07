# Pull R Base image from Amazon Public ECR Gallery
FROM public.ecr.aws/docker/library/r-base:4.2.2

RUN  apt update -y 
RUN apt-get -y install libcurl4-openssl-dev apt-transport-https libsodium-dev libxml2-dev libssl-dev 
RUN apt -y install default-jre   

COPY usedLibraries.R  /opt/ml/processing/usedLibraries.R 

RUN Rscript /opt/ml/processing/usedLibraries.R

ENTRYPOINT ["Rscript", "/opt/ml/processing/input/Run_predictcash_test_env.R"]  
