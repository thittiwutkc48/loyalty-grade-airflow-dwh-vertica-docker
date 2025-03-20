# Install tzdata to configure the timezone
RUN apt-get update && \
    apt-get install -y tzdata

# Set the timezone to Asia/Bangkok
ENV TZ=Asia/Bangkok
RUN ln -sf /usr/share/zoneinfo/Asia/Bangkok /etc/localtime && \
    dpkg-reconfigure --frontend noninteractive tzdata
