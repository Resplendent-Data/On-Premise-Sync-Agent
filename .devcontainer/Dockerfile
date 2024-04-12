# This is the development container.
FROM python:3.11-bullseye

# Setup user
ARG USERNAME=sync-agent
ARG USER_UID=1000
ARG USER_GID=$USER_UID
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    #
    # [Optional] Add sudo support. Omit if you don't need to install software after connecting.
    && apt-get update \
    && apt-get install -y sudo \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME \
    # Add the docker group to the user.
    && groupadd docker \
    # Add the user to the docker group.
    && usermod -aG docker $USERNAME \
    # Add the /var/run directory to the docker group.
    && chown $USERNAME:docker /var/run/

# Install zsh, git, and curl
RUN apt-get update && apt-get install -y zsh git curl

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

USER $USERNAME
ENV PATH="/home/${USERNAME}/.local/bin:${PATH}"

WORKDIR /workspace

# Make the user the owner of the home directory and of the command history directory.
RUN sudo chown -R $USERNAME /home/$USERNAME
RUN sudo chown -R $USERNAME /workspace

# Install dependencies
COPY pyproject.toml /workspace
COPY poetry.lock /workspace
RUN pip install poetry
RUN poetry install


# Install oh-my-zsh
# Default powerline10k theme, no plugins installed
RUN sh -c "$(wget -O- https://github.com/deluan/zsh-in-docker/releases/download/v1.1.2/zsh-in-docker.sh)"

ENV SHELL /bin/zsh

RUN SNIPPET="export PROMPT_COMMAND='history -a' && export HISTFILE=/commandhistory/.zsh_history" \
    && sudo mkdir /commandhistory \
    && sudo touch /commandhistory/.zsh_history \
    && sudo chown -R $USERNAME /commandhistory \
    && echo $SNIPPET >> "/home/$USERNAME/.zshrc"

# Copy entrypoint script into the image
COPY .devcontainer/entrypoint.sh /entrypoint.sh
RUN sudo chmod +x /entrypoint.sh

# Set the script as the entrypoint
ENTRYPOINT ["/entrypoint.sh"]