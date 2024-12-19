# Welcome to CS 165 Docker Procedure
#
# Most people should not have to change this too much. Should you need
# customization for development reasons, see the Customization section of this
# Dockerfile near the bottom of the file.

FROM ubuntu:22.04

WORKDIR /cs165

###################### Begin STAFF ONLY ##########################

# These statements are for the staff to set up environment on lab machines. They
# should not be modified by students in ways that would cause the corresponding
# layers to be rebuilt, since that would slow down the build process.

ENV DEBIAN_FRONTEND=noninteractive

RUN bash -c "set -ex; \
  apt-get update && \
  apt-get install -y software-properties-common && \
  add-apt-repository ppa:deadsnakes/ppa"

RUN bash -c "set -ex; \
  apt-get update && \
  apt-get install -y \
    apt-utils build-essential gcc curl psmisc python3.11 python3.11-distutils \
    tmux valgrind strace vim mutt bc"

RUN bash -c "set -ex; \
  curl -sSL https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
  python3.11 get-pip.py"

COPY requirements.txt .

RUN bash -c "python3.11 -m pip install -r requirements.txt"

RUN bash -c "set -ex; \
  apt-get install -y linux-tools-common linux-tools-generic && \
  cd /usr/lib/linux-tools && \
  cd \$(ls -1 | head -n1) && \
  rm -f /usr/bin/perf && \
  ln -s \$(pwd)/perf /usr/bin/perf && \
  ln -s /usr/bin/python3.9 /usr/bin/python"

###################### End STAFF ONLY ##########################

###################### Begin Customization ########################

ENV RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/usr/local/cargo \
  PATH=/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

RUN bash -c "set -ex && \
  curl --proto '=https' --tlsv1.2 https://sh.rustup.rs -sSf | \
    sh -s -- --default-toolchain=nightly-2024-11-28 -y && \
  chmod -R a+w $RUSTUP_HOME $CARGO_HOME"

COPY dev-requirements.txt ./

RUN bash -c "set -ex && \
  python3.11 -m pip install -r dev-requirements.txt"

###################### End Customization ##########################

# Start by cleaning and making, and then starting a shell with tmux. Tmux is a
# nice window multiplexer/manager within a single terminal. For a quick rundown
# of tmux window management shortcuts: https://tmuxcheatsheet.com/
CMD [ "/bin/bash", "-c", "make clean && make && tmux" ]
