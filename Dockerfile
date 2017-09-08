FROM php:7.1
MAINTAINER Erik Zigo <erik.zigo@keboola.com>

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update \
  && apt-get install unzip git unixODBC-dev libpq-dev -y

RUN echo "memory_limit = -1" >> /usr/local/etc/php/php.ini
RUN echo "date.timezone = \"UTC\"" >> /usr/local/etc/php/php.ini

RUN docker-php-ext-install pdo_pgsql pdo_mysql

# Install PHP odbc extension
RUN set -x \
    && docker-php-source extract \
    && cd /usr/src/php/ext/odbc \
    && phpize \
    && sed -ri 's@^ *test +"\$PHP_.*" *= *"no" *&& *PHP_.*=yes *$@#&@g' configure \
    && ./configure --with-unixODBC=shared,/usr \
    && docker-php-ext-install odbc \
    && docker-php-source delete

## install snowflake drivers
ADD snowflake_linux_x8664_odbc.tgz /usr/bin
ADD ./driver/simba.snowflake.ini /etc/simba.snowflake.ini
ADD ./driver/odbcinst.ini /etc/odbcinst.ini
RUN mkdir -p  /usr/bin/snowflake_odbc/log

ENV SIMBAINI /etc/simba.snowflake.ini
ENV SSL_DIR /usr/bin/snowflake_odbc/SSLCertificates/nssdb
ENV LD_LIBRARY_PATH /usr/bin/snowflake_odbc/lib

#  charset settings
ENV LANG en_US.UTF-8
ENV LC_ALL=C.UTF-8

COPY . /code/
WORKDIR /code

RUN curl -sS https://getcomposer.org/installer | php \
  && mv /code/composer.phar /usr/local/bin/composer \
  && composer install

CMD php ./run.php --data=/data

