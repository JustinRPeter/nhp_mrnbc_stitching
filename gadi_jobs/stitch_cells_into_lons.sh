#!/bin/bash

# This script generates and submits jobs for stitching grid cell .nc files into
# longitudinal strips. Each longitudinal strips is actually 3 lons, and all
# lat values for those lons.
# Multiple jobs are submitted for one gcm/rcp, with each job repsonsible for
# a range of lon triplets.
# Use SKIP for a value in lon_triplet_start_indices to skip that lon group.
# This can be useful for when one of the lon group jobs fails, and you want
# to run just that group again, whilst preserving the group number in the job logs.
# Sometimes, one lon group will hang and the job will be killed when it
# reaches the walltime. Change all lon group start index values to SKIP for
# all except for the groups to re-run.
set -e

gcms=(ACCESS1-0 CNRM-CM5 GFDL-ESM2M MIROC5)
rcps=(historical rcp45 rcp85)


template_file=stitch_cells_into_lons.template
lon_triplet_start_indices=(0 50 100 150 200 250)
lon_triplet_end_indices=(49 99 149 199 249 280)
num_lon_triplet_groups=6

for gcm in ${gcms[@]}; do
    for rcp in ${rcps[@]}; do
        for ((lon_group=0;lon_group<${num_lon_triplet_groups};lon_group++)); do
            lon_start_group=${lon_triplet_start_indices[$lon_group]}
            lon_end_group=${lon_triplet_end_indices[$lon_group]}

            if [ "${lon_start_group}" = "SKIP" ]; then
                echo "Skipping ${gcm} ${rcp} lon group ${lon_group}"
                continue
            fi

            job_file=generated_job_mrnbc_stitch_cells_to_lons_${gcm}_${rcp}_lon_triplet_${lon_group}.pbs
            job_name=job_mrnbc_stitch_cells_to_lons_${gcm}_${rcp}_lon_triplet_${lon_group}

            # Create job file from template
            echo "Creating Job ${job_file}"
            cp ${template_file} ${job_file}
            sed -i "s|xxJOB_NAMExx|${job_name}|g" ${job_file}
            sed -i "s|xxGCMxx|${gcm}|g" ${job_file}
            sed -i "s|xxRCPxx|${rcp}|g" ${job_file}
            sed -i "s|xxLON_START_GROUPxx|${lon_start_group}|g" ${job_file}
            sed -i "s|xxLON_END_GROUPxx|${lon_end_group}|g" ${job_file}
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
