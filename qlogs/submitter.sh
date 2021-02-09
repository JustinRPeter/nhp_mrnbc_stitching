start=( 0 50 100 150 200 250 )
end=( 50 100 150 200 250 281 )

for (( ii=0;ii<=7;ii++ )); do
cat >> temp_submission_script <<EOF
#!/bin/bash

#PBS -q hugemem
#PBS -l walltime=48:00:00
#PBS -l storage=gdata/er4+scratch/er4+scratch/eg3+gdata/eg3
#PBS -N job_hugemem_mrnbc
#PBS -P er4
#PBS -l ncpus=26
#PBS -l mem=1440gb
#PBS -l wd

gcm=CNRM-CM5
rcp=rcp45
input_base_dir=/scratch/eg3/${USER}/mrnbc_stitching/text_to_nc
output_base_dir=/scratch/eg3/${USER}/mrnbc_stitching/pre_stitch

source /g/data/er4/jr6311/miniconda/bin/activate isimip

cd ../
python3 awap_stitch.py \
    --gcm ${gcm} \
    --rcp ${rcp} \
    --start ${start[$ii]} \
    --end ${end[$ii]} \
    --input_base_dir ${input_base_dir} \
    --output_base_dir ${output_base_dir}

EOF

qsub temp_submission_script
rm temp_submission_script
sleep 2
done
