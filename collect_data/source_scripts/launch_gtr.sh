function GETDATA(){
    for ((OFFSET=$1;OFFSET<=10;++OFFSET));
    do
	_NAME=$(echo "USE tier_0; SELECT name FROM gtr_organisations LIMIT 1 OFFSET $OFFSET;" | mysql --defaults-extra-file=/Users/hep/Nesta/nesta_dataflow/db_config/tier-0.cnf | tail -n 1)
	_ID=$(echo "USE tier_0; SELECT id FROM gtr_organisations LIMIT 1 OFFSET $OFFSET;" | mysql --defaults-extra-file=/Users/hep/Nesta/nesta_dataflow/db_config/tier-0.cnf| tail -n 1)
	_URL=$(echo "USE tier_0; SELECT url FROM gtr_organisations LIMIT 1 OFFSET $OFFSET;" | mysql --defaults-extra-file=/Users/hep/Nesta/nesta_dataflow/db_config/tier-0.cnf| tail -n 1)
	echo -e $_NAME"\t"$_ID"\t"$_URL >> input.tsv
    done    
}

# Generate the environment
source aws-tools/deploy.sh --script gtr_extrainfo_aws --refresh --scriptpath utils/immerseuk/gtr/aws/ --config config/immerseuk_gtr_aws/ -re --dryrun
rm input.tsv &> /dev/null

# Generate data file
OFFSET=0
RESULTS="DUMMY"
while [[ $RESULTS != "" ]];
do
    GETDATA $OFFSET
    OFFSET=$((OFFSET+10))
    zip env/gtr_extrainfo_aws.zip input.tsv
    #source aws-tools/deploy.sh --script gtr_extrainfo_aws --refresh --scriptpath utils/immerseuk/gtr/aws/ --config config/immerseuk_gtr_aws/
    break
done

