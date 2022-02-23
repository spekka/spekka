FROM gitpod/workspace-base

SHELL ["/bin/bash", "-c"]
USER gitpod

RUN curl -s "https://get.sdkman.io" | bash
RUN . /home/gitpod/.sdkman/bin/sdkman-init.sh && \
    sdk selfupdate force && \
    sdk install java $(sdk list java | grep -oE "8\.\S+-tem" | head -1) && \
    sdk install java $(sdk list java | grep -oE "11\.\S+-tem" | head -1) && \
    sdk install sbt

RUN mkdir -p /home/gitpod/.local/share/bash-completion/completions

RUN cd /tmp && curl -fLo cs https://git.io/coursier-cli-"$(uname | tr LD ld)" && \
    chmod a+x ./cs && \
    ./cs install cs && \
    echo "export PATH=\"$PATH:/home/gitpod/.local/share/coursier/bin\"" > ~/.bashrc.d/coursier && \
    ./cs install bloop ammonite && \
    curl -o /home/gitpod/.local/share/bash-completion/completions/bloop https://raw.githubusercontent.com/scalacenter/bloop/master/etc/bash-completions
    