FROM crisbal/torch-rnn:base

RUN apt-get update && \
    apt-get install screen htop