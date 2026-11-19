FROM python:3.11

WORKDIR /home/app

RUN apt-get update \
    && apt-get install -y --no-install-recommends nano unzip curl \
    && rm -rf /var/lib/apt/lists/*

COPY . . 

RUN curl -fsSL https://get.deta.dev/cli.sh | sh

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install

# # THIS IS SPECIFIC TO HUGGINFACE
# # We create a new user named "user" with ID of 1000
# RUN useradd -m -u 1000 user
# # We switch from "root" (default user when creating an image) to "user" 
# USER user
# # We set two environmnet variables 
# # so that we can give ownership to all files in there afterwards
# # we also add /home/user/.local/bin in the $PATH environment variable 
# # PATH environment variable sets paths to look for installed binaries
# # We update it so that Linux knows where to look for binaries if we were to install them with "user".
# ENV HOME=/home/user \
#     PATH=/home/user/.local/bin:$PATH

# # We set working directory to $HOME/app (<=> /home/user/app)
# WORKDIR $HOME/app

# # Copy all local files to /home/user/app with "user" as owner of these files
# # Always use --chown=user when using HUGGINGFACE to avoid permission errors
# COPY --chown=user . $HOME/app

COPY requirements.txt /dependencies/requirements.txt
RUN pip install -r /dependencies/requirements.txt

ENV PYTHONWARNINGS=ignore::DeprecationWarning,ignore::PendingDeprecationWarning

ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
ENV AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY

ENV MLFLOW_BACKEND_STORE_URI=$MLFLOW_BACKEND_STORE_URI
ENV MLFLOW_ARTIFACT_ROOT=$MLFLOW_ARTIFACT_ROOT
ENV MLFLOW_HOST=0.0.0.0
ENV MLFLOW_PORT=5000

#ENV MLFLOW_ALLOWED_HOSTS=$MLFLOW_ALLOWED_HOSTS #"localhost,127.0.0.1"

EXPOSE 5000

# --serve-artifacts : the SERVER writes the artifacts, the client POSTs them
# over HTTP. Without it an MLflow client writes to --default-artifact-root with
# its OWN filesystem, and the Airflow container (uid 50000) cannot create a
# directory in a volume the mlflow container made:
#     PermissionError: [Errno 13] Permission denied: '/mlruns/artifacts'
#
# Mounting the volume into Airflow would NOT have been enough -- that was the
# first attempt -- because the uids differ. Proxying removes the shared
# filesystem from the problem rather than patching its permissions, and it is
# also the form this would take on more than one machine.
CMD mlflow server \
      --backend-store-uri ${MLFLOW_BACKEND_STORE_URI} \
      --serve-artifacts \
      --artifacts-destination ${MLFLOW_ARTIFACT_ROOT} \
      --default-artifact-root mlflow-artifacts:/ \
      --host ${MLFLOW_HOST} \
      --port ${MLFLOW_PORT} \
      --allowed-hosts "*"