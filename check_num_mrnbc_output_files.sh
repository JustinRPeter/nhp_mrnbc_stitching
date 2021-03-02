#!/bin/bash

set -e

gcm=ACCESS1-0
rcp=rcp45
period=bc_fut
input_base_dir=/scratch/eg3/jp0715/HydroProj/data/unsw/mrnbc_output/awap_res
time_slices=(2006-2035 2036-2065 2066-2095 2070-2099)


time_slice_base_input_dir=${input_base_dir}/${gcm}/${rcp}

for time_slice in ${time_slices[@]}; do
    echo "Finding files for ${gcm} ${rcp} ${time_slice}"
    find_results_file=temp_find_results_${gcm}_${rcp}_${time_slice}.txt

    find \
        ${time_slice_base_input_dir}/${time_slice} \
        -maxdepth 1 \
        -type f \
        -name "${period}_*.dat" \
        -printf "%f\n" \
        > ${find_results_file}

    echo "Checking if all grid cells present"
    for ((i=0;i<841;i++)); do
        num_lats=$(grep "${period}_${i}_.*" ${find_results_file} | wc -l)
        if [ ${num_lats} -ne "681" ]; then
            echo "  Lon ${i}, Lats = ${num_lats}"
        fi
    done

    rm ${find_results_file}

    if [ "${period}" = "bc_cur" ]; then
        break
    fi
done

echo "Done"
