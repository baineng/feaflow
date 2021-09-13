#!/usr/bin/env bash

if [ ! -x "$(command -v pip 2>/dev/null)" ]; then
  echo 'can not find "pip" in your system.'
  exit 1
fi

usage()
{
  echo "Usage: install.sh [options] <extra_packages>"
  echo ""
  echo "Options:"
  echo "-h, --help       Show usage"
  echo "-e, --editable   Install in editable mode"
  echo ""
  echo "<extra_packages> Extra packages to be installed, e.g. "airflow", "spark" or "airflow,spark""
}

EDITABLE=""
EXTRA=""

while [ "$1" != "" ]; do
  case $1 in
  -h | --help) usage; exit ;;
  -e | --editable) EDITABLE="true"; shift 1; ;;
  *)
    EXTRA="$1";
    shift
    ;;
  esac
done

ROOT_DIR=$(dirname $(dirname "$(realpath "$0")"))
cd $ROOT_DIR || exit 1

if [ "$EXTRA" = "" ]; then
  PACKAGE="."
else
  PACKAGE=".[$EXTRA]"
fi

if [ "$EDITABLE" = "true" ]; then
  pip install -e $PACKAGE
else
  pip install $PACKAGE
fi
