FROM golang:1.24

# Install required packages
RUN apt-get update && apt-get install -y \
    # Required for make test
    make \
    # Required to run bash tests
    bash \
    # Required for fixtures
    python3 \
    # Required for testing
    git \
    # Required for wget
    wget \
    # Required for mv operations
    sudo \
    # Required for Kafka
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Kafka
RUN wget --progress=dot:giga https://media.githubusercontent.com/media/codecrafters-io/build-your-own-kafka/main/kafka_2.13-4.0.0-SNAPSHOT.tgz
RUN tar -xzf kafka_2.13-4.0.0-SNAPSHOT.tgz
RUN sudo mv kafka_2.13-4.0.0-SNAPSHOT/ /usr/local/kafka-latest
ENV PATH=$PATH:/usr/local/kafka-latest/bin
RUN rm kafka_2.13-4.0.0-SNAPSHOT.tgz

# Starting from Go 1.20, the go standard library is no longer compiled.
# Setting GODEBUG to "installgoroot=all" restores the old behavior
RUN GODEBUG="installgoroot=all" go install std

# Copy go.mod and go.sum first to cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Default command
CMD ["/bin/bash"]