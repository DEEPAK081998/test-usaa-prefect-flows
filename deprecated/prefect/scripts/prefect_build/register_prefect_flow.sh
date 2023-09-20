#!/bin/bash

usage() {
  echo "Usage: sh $0 -p <project-name> [-a <aws-profile>] -f <file-path>" 1>&2
  exit 1
}

while getopts "p:a::f:" flag; do 
  case "${flag}" in
    p) project=${OPTARG} ;;
    a) profile=${OPTARG} ;;
    f) path=${OPTARG} ;;
    *) usage ;;
  esac
done

if [ -z "${project}" ] || [ -z "${path}" ]; then
  usage
fi

echo "Registering prefect flows ..."

if [ -n "${profile}" ]; then
  export AWS_PROFILE="${profile}"
fi

prefect register --project "${project}" -p "${path}"

if [ -n "${profile}" ]; then
  unset AWS_PROFILE
fi