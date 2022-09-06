#!/bin/bash

data_size=$1
bucket=$2
data_format=$3

if [[ -z "$data_size"  ]]; then 
	echo "input for data size is null, exiting loader utility."
	exit;
fi

if [[ -z "$bucket"  ]]; then
	echo "input for gcs bucket is null, exiting loader utility"
	exit;
fi

if [[ -z "data_format" ]]; then
	echo "input for data format is null. Parquet will be generated"
fi

##clone generator
echo 'Downloading TPCH data generator from git...'
git clone https://github.com/jster1357/tpch-generator.git
##change directories
cd datagen/
echo 'running data generation...'
./datagen.sh "$data_size" "$bucket" "$format"
