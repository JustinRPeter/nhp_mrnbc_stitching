#!/bin/bash

# This script generates and submits jobs for the final step of stitching.
# A job is generated for each GCM/RCP combo.
# Each job will merge all the lon strips created in the previous step.
# The files up until this point contain all variables. This job will
# output one file per variable.
set -e

gcms=(ACCESS1-0 CNRM-CM5 GFDL-ESM2M MIROC5)
rcps=(historical rcp45 rcp85)


template_file=stitch_final.template

for gcm in ${gcms[@]}; do
    for rcp in ${rcps[@]}; do
        job_file=generated_job_mrnbc_stitch_all_${gcm}_${rcp}.pbs
        job_name=job_mrnbc_stitch_all_${gcm}_${rcp}

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
