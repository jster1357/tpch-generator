#!/bin/bash

data_size=$1
bucket=$2

if [[ -z "$data_size"  ]]; then 
	echo "input for data size is null, exiting loader utility."
	exit;
fi

if [[ -z "$bucket"  ]]; then
	echo "input for gcs bucket is null, exiting loader utility"
	exit;
fi

##clone generator
echo 'Downloading TPCH data generator from git...'
git clone https://github.com/jster1357/tpch-generator.git
##change directories
cd tpch-generator/datagen/
echo 'running data generation...'
./datagen.sh "$data_size" "$bucket"
