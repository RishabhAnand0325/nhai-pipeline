FROM ubuntu:22.04

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

# Install system dependencies including Python 3.9 and ffmpeg
RUN apt-get update && \
    apt-get install -y \
    software-properties-common \
    && add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y \
    python3.9 \
    python3.9-dev \
    python3.9-venv \
    python3.9-distutils \
    python3-pip \
    ffmpeg \
    git \
    wget \
    curl \
    unzip \
    ca-certificates \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set Python 3.9 as default
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 1 && \
    update-alternatives --install /usr/bin/python python /usr/bin/python3.9 1

# Install pip for Python 3.9
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.9

# Set working directory
WORKDIR /app

# Copy requirements files
COPY requirements.txt ./
COPY annotation-pipeline/requirements.txt ./annotation-pipeline/

# Install Python dependencies
RUN python3.9 -m pip install --no-cache-dir -r requirements.txt && \
    python3.9 -m pip install --no-cache-dir -r annotation-pipeline/requirements.txt

# Copy all pipeline source code into the image
COPY main.py ./
COPY pipeline1.py ./
COPY annotation-pipeline/ ./annotation-pipeline/
COPY annotation-pipeline-bak/ ./annotation-pipeline-bak/

CMD ["python3.9", "main.py"]