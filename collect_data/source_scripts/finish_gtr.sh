PROFILE=default
NBATCH=6

function GETDATA(){
    _OFFSET=$1
    _NBATCH=$2
    MAXOFFSET=$((_OFFSET+_NBATCH-1))
    rm input.tsv &> /dev/null	    
    for ((TOFFSET=$_OFFSET;TOFFSET<=$MAXOFFSET;++TOFFSET));
    do
	_NAME=$(echo "USE tier_0; SELECT name FROM gtr_organisations LIMIT 1 OFFSET $TOFFSET;" | mysql --defaults-extra-file=/Users/hep/Nesta/nesta_dataflow/db_config/tier-0.cnf | tail -n 1)
	_ID=$(echo "USE tier_0; SELECT id FROM gtr_organisations LIMIT 1 OFFSET $TOFFSET;" | mysql --defaults-extra-file=/Users/hep/Nesta/nesta_dataflow/db_config/tier-0.cnf| tail -n 1)
	_URL=$(echo "USE tier_0; SELECT url FROM gtr_organisations LIMIT 1 OFFSET $TOFFSET;" | mysql --defaults-extra-file=/Users/hep/Nesta/nesta_dataflow/db_config/tier-0.cnf| tail -n 1)
	echo -e $_NAME"\t"$_ID"\t"$_URL >> input.tsv
    done    
}

# Generate the environment
#source aws-tools/deploy.sh --script gtr_extrainfo_aws --refresh --scriptpath utils/immerseuk/gtr/aws/ --config config/immerseuk_gtr_aws/ -re --dryrun
#source aws-tools/deploy.sh --script gtr_extrainfo_aws --refresh --scriptpath utils/immerseuk/gtr/aws/ --config config/immerseuk_gtr_aws/ --dryrun

# Generate data file
OFFSET=0
RESULTS=$(echo "USE tier_0; SELECT count(*) FROM gtr_organisations;" | mysql --defaults-extra-file=/Users/hep/Nesta/nesta_dataflow/db_config/tier-0.cnf | tail -n 1)
echo "There are $RESULTS results"
while [[ $OFFSET -lt $RESULTS ]];
do
    #echo -n $OFFSET &> "out-id"
    #cat out-id
    #echo ""

    OFFSET=$((OFFSET+NBATCH))
    #zip env/gtr_extrainfo_aws.zip input.tsv
    #zip env/gtr_extrainfo_aws.zip out-id

    
    
    line=$(cat source_scripts/not_done)
    arr=($line)
    for i in "${arr[@]}";
    do
	if [[ $i -eq $OFFSET ]];
	then
	    echo gtr_$OFFSET $i
	    GETDATA $OFFSET 3
	    mv input.tsv gtr_${OFFSET}_1

	    GETDATA $((OFFSET+3)) 3
	    mv input.tsv gtr_${OFFSET}_2

	    aws s3 cp gtr_${OFFSET}_1 s3://tier-0-inputs --profile $PROFILE
	    aws s3 cp gtr_${OFFSET}_2 s3://tier-0-inputs --profile $PROFILE
	    
	    rm gtr_${OFFSET}_1 &> /dev/null
	    rm gtr_${OFFSET}_2 &> /dev/null

	    break
	fi
    done
    
    
    #cat gtr_$OFFSET
    #echo ""


    
    # Tidy up
    #rm input.tsv &> /dev/null
    #rm gtr_$OFFSET &> /dev/null
    # Testing

    #if [[ $OFFSET -gt 20 ]];
    #then
    #	break
    #    fi
done

