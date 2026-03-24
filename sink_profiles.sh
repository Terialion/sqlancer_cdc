#!/usr/bin/env bash

# Central place to define sink runtime profiles.
# Add a new sink by extending resolve_sink_profile().

resolve_sink_profile() {
  local sink_type_raw="$1"
  local sink_type
  sink_type="$(echo "${sink_type_raw}" | tr '[:upper:]' '[:lower:]')"

  SINK_SERVICE=""
  SINK_LABEL="${sink_type}"
  SINK_DB_PORT=""
  SINK_IMAGE=""
  SINK_SQL_ENABLED="0"

  case "${sink_type}" in
    doris)
      SINK_SERVICE="doris"
      SINK_LABEL="Doris"
      SINK_DB_PORT="9030"
      SINK_IMAGE="apache/doris:doris-all-in-one-2.1.0"
      SINK_SQL_ENABLED="1"
      ;;
    starrocks)
      SINK_SERVICE="starrocks"
      SINK_LABEL="StarRocks"
      SINK_DB_PORT="9030"
      SINK_IMAGE="starrocks/allin1-ubuntu:3.5.10"
      SINK_SQL_ENABLED="1"
      ;;
    paimon)
      SINK_LABEL="Paimon"
      SINK_SQL_ENABLED="0"
      ;;
    *)
      return 1
      ;;
  esac

  return 0
}
