#!/bin/bash

# This script generates and submits jobs for merging mrnbc .dat files that
# are split over multiple time slices.
# For each GCM/RCP combo, multiple jobs are generated/submitted, grouped
# into about ~100 longitudinal strips per job. Each lon group does all
# lat grids (0-680).
# Note: it is not necessary to merge time slices for historical time step, because
# the full time series is already available in the one file.
set -e


gcms=(ACCESS1-0 CNRM-CM5 GFDL-ESM2M MIROC5)
rcps=(rcp45 rcp85)


template_file=stitch_text_time_slices.template
lon_start_indices=(0 100 200 300 400 500 600 700 800)
lon_end_indices=(99 199 299 399 499 599 699 799 840)
num_lon_groups=9
lat_start_index=0
lat_end_index=680

for gcm in ${gcms[@]}; do
    for rcp in ${rcps[@]}; do
        for ((lon_group=0;lon_group<${num_lon_groups};lon_group++)); do
            lon_start_index=${lon_start_indices[$lon_group]}
            lon_end_index=${lon_end_indices[$lon_group]}

            job_file=generated_job_mrnbc_stitch_text_time_slices_${gcm}_${rcp}_lon_group_${lon_group}.pbs
            job_name=job_mrnbc_stitch_text_time_slices_${gcm}_${rcp}_lon_group_${lon_group}

            # Create job file from template
            echo "Creating Job ${job_file}"
            cp ${template_file} ${job_file}
            sed -i "s|xxJOB_NAMExx|${job_name}|g" ${job_file}
            sed -i "s|xxGCMxx|${gcm}|g" ${job_file}
            sed -i "s|xxRCPxx|${rcp}|g" ${job_file}
            sed -i "s|xxLON_START_INDEXxx|${lon_start_index}|g" ${job_file}
            sed -i "s|xxLON_END_INDEXxx|${lon_end_index}|g" ${job_file}
            sed -i "s|xxLAT_START_INDEXxx|${lat_start_index}|g" ${job_file}
            sed -i "s|xxLAT_END_INDEXxx|${lat_end_index}|g" ${job_file}
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
done
