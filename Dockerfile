FROM python:3.11-slim-bookworm
EXPOSE 8050

ENV DEBIAN_FRONTEND=noninteractive

# Install essential packages
RUN apt update && apt install -y \
    apt-utils curl ca-certificates software-properties-common apt-transport-https debconf-utils gcc build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN apt update && apt install -y \
    python3-pip python3-dev python3-setuptools \
    libpq-dev \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Add Microsoft SQL driver repository
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list

# Install ODBC drivers
RUN apt update -y \
    && ACCEPT_EULA=Y apt install -y msodbcsql17 \
    && apt install -y unixodbc-dev \
    && apt install -y libgssapi-krb5-2 \
    && apt install -y python3-mysqldb default-libmysqlclient-dev libssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies using poetry
COPY pyproject.toml /app
COPY poetry.lock /app
RUN pip install poetry
RUN poetry install

# Copy entrypoint script into the image
COPY .devcontainer/entrypoint-prod.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Set the script as the entrypoint
ENTRYPOINT ["/entrypoint.sh"]

COPY . /app

# Command to run on container start
CMD [ "./start_sync_agent_and_webpage.sh" ]
