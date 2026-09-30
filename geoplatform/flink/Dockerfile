FROM flink:1.18-scala_2.12

# Install Python3 and pip
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    rm -rf /var/lib/apt/lists/*

# Link python to python3
RUN ln -s /usr/bin/python3 /usr/bin/python

# Install dependencies
COPY requirements.txt /opt/flink/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/flink/requirements.txt
