#!/bin/bash

xls_path="${XL_IDP_PATH_NW_FEA}"

done_path="${xls_path}"/done
if [ ! -d "$done_path" ]; then
  mkdir "${done_path}"
fi

json_path="${xls_path}"/json
if [ ! -d "$json_path" ]; then
  mkdir "${json_path}"
fi

find "${xls_path}" -maxdepth 1 -type f \( -name "*.xls*" -or -name "*.XLS*" \) ! -newermt '3 seconds ago' -print0 | while read -d $'\0' file
do

  if [[ "${file}" == *"error_"* ]];
  then
    continue
  fi

  echo "Will convert XLS* '${file}' to JSON '${json_path}'"
  python3 "${XL_IDP_ROOT_NW_FEA}/scripts/main.py" "${file}" "${json_path}"

  if [ $? -eq 0 ]
    then
      mv "${file}" "${done_path}"
    else
      echo "ERROR during convertion ${file} to json!"
      mv "${file}" "${xls_path}/error_$(basename "${file}")"
      continue
    fi

done
