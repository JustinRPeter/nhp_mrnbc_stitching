#!/bin/bash

# This script generates and submits jobs for converting
# MRNBC .dat files to NetCDF files, one per grid cell.
# For each GCM/RCP combo, one job is generated and submitted.
# The job will internally run with a pool of threads, handling one grid cell each.
set -e


gcms=(ACCESS1-0 CNRM-CM5 GFDL-ESM2M MIROC5)
rcps=(historical rcp45 rcp85)


template_file=convert_text_to_nc.template

for gcm in ${gcms[@]}; do
    for rcp in ${rcps[@]}; do
        job_file=generated_job_mrnbc_text_to_nc_${gcm}_${rcp}.pbs
        job_name=job_mrnbc_text_to_nc_${gcm}_${rcp}

        # Create job file from template
        echo "Creating Job ${job_file}"
        cp ${template_file} ${job_file}
        sed -i "s|xxJOB_NAMExx|${job_name}|g" ${job_file}
        sed -i "s|xxGCMxx|${gcm}|g" ${job_file}
        sed -i "s|xxRCPxx|${rcp}|g" ${job_file}
        wait

        # submit job
        echo "Submitting Job ${job_file}"
        qsub ${job_file}
        wait

        echo "Removing Job file"
        rm ${job_file}
        wait
    done
done
