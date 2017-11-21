#!/usr/bin/env bash

composer selfupdate
composer install -n

export ROOT_PATH="/code";

./vendor/bin/phpunit
