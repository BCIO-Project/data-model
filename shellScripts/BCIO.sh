#!/bin/bash

PATH_PROCESOS=`dirname $0`

#source debe ser uno de estos:
#   - GetInfoAPI
#   - EscrituraModeloRedis
#   - Modelo30DaysCampaignInfoCTR

# Parametros por defecto
cluster=dataprisaglobalstratiodes
region=europe-west1

# Sobre escribe parametros por defecto con valores de entrada si existen
for ar in $@ ; do
   K=${ar%=*}
   V=${ar##*=}
   export $K=$V
done


# Parametros obligatorios
for K in source jarpath; do
  eval V=\$$K
  if [ -z "$V" ]; then
    echo "El parametro $K es requerido"
    exit 1
  fi
done

if [[ -n "${fechaParam/[ ]*\n/}" ]];
then
	fecha_process=$(date -d "$fechaParam" +'%Y-%m-%d')
else
	fecha_process=$(/bin/date +%Y-%m-%d)
fi


# Common definitions
conf=BCIO.properties
confFilePath="${PATH_PROCESOS}/${conf}"

echo -e "\nValor variable confFilePath = " $confFilePath

launch_job_for_source()
{
  process="BCIO"
  # ***********************  Source: params *******************
  source=$1
  CapitalProcessName="$source"

  echo "Process: ${process}"
  echo "Source: ${source}"
  BCIOProcess=$process-$CapitalProcessName

  echo "Launching: $BCIOProcess"
  echo gcloud dataproc jobs submit spark --cluster $cluster --region $region --driver-log-levels root=INFO --jar $jarpath --files $confFilePath --labels=job_id=${BCIOProcess,,} --  --source $source --conf $conf --date $fecha_process
  gcloud dataproc jobs submit spark --cluster $cluster --region $region --driver-log-levels root=INFO --jar $jarpath --files $confFilePath --labels=job_id=${BCIOProcess,,} --  --source $source --conf $conf --date $fecha_process

}

echo "Lanzamos la ingesta para la fuente: $source"
launch_job_for_source $source