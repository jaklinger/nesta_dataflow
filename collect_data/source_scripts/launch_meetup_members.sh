COUNTRY=$1
COUNTRY_CODE=$2
CATEGORY=$3

PROFILE=default
PREFIX="meetup_${COUNTRY// /_}_${CATEGORY}_"

function LAUNCH(){    
    MEMBER_ID=$1
    touch $PREFIX$MEMBER_ID	  
    RESPONSE=$(aws s3 cp $PREFIX$MEMBER_ID s3://tier-0-inputs --profile $PROFILE)    
    rm $PREFIX$MEMBER_ID &> /dev/null
    if [[ $RESPONSE == *failed* ]];
    then
	echo $RESPONSE
	echo $PREFIX$MEMBER_ID >> failed_tasks
    else
	echo $MEMBER_ID" submitted"
    fi
}

#LINEDATA=$(cat /Users/jklinger/Nesta/adhoc_data/member_ids_saudi_arabia.txt)
_QUERY="USE tier_0; SELECT distinct(member_id) FROM meetup_groups_members WHERE group_id IN (SELECT id FROM meetup_groups WHERE country = '${COUNTRY_CODE}' AND category_id = ${CATEGORY});"
_CNF=/Users/$USER/Nesta/nesta_dataflow/db_config/tier-0.cnf
RESULTS=$(echo $_QUERY | mysql --defaults-extra-file=$_CNF)

ILINE=0 # Global counter
JLINE=0 # Flush counter
for MEMBER_ID in $RESULTS; do
    ILINE=$((ILINE+1))    
    JLINE=$((JLINE+1))
    if [[ $ILINE -le 1 ]]; then
	continue
    fi

    LAUNCH $MEMBER_ID &
    #aws s3 rm s3://tier-0/meetup_$MEMBER_ID --quiet & 
    
    if [[ $JLINE -ge 30 ]]; then
	JLINE=0
	echo "Sleeping"
	sleep 5
    fi
    
    #if [[ $ILINE -ge 100 ]]; then
    #	echo $MEMBER_ID
    #	break
    #    fi
done 

echo "Done"
