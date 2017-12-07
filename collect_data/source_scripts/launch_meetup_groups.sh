#COUNTRY=$1
#COUNTRY_CODE=$2
#CATEGORY=$3

PROFILE=default
PREFIX="group_meetup_241117_" #${COUNTRY// /_}_${CATEGORY}_"


function LAUNCH(){
    GROUP_ID=$1

    touch $PREFIX$GROUP_ID
    RESPONSE=$(aws s3 cp $PREFIX$GROUP_ID s3://tier-0-inputs --profile $PROFILE)
    rm $PREFIX$GROUP_ID &> /dev/null
    if [[ $RESPONSE == *failed* ]];
    then
        echo $RESPONSE
        echo $PREFIX$GROUP_ID >> failed_tasks
    else
        echo $GROUP_ID" submitted"
    fi
}

#_QUERY="USE tier_0; SELECT distinct(group_urlname) FROM meetup_groups_members WHERE group_id NOT IN (SELECT id FROM meetup_groups WHERE country = '${COUNTRY_CODE}' AND category_id = ${CATEGORY});"
_QUERY="SELECT distinct(group_urlname) FROM meetup_groups_events WHERE event_id NOT IN (SELECT distinct(event_id) FROM meetup_events_members);"
RESULTS=$(echo $_QUERY | mysql --defaults-extra-file=/Users/$USER/Nesta/nesta_dataflow/db_config/tier-0.cnf)

ILINE=0
JLINE=0
for GROUP_ID in $RESULTS; do
    ILINE=$((ILINE+1))
    JLINE=$((JLINE+1))
    if [[ $ILINE -le 1 ]]; then
	continue
    fi

    LAUNCH $GROUP_ID &
    #aws s3 rm s3://tier-0/meetup_$GROUP_ID --quiet &     
    
    if [[ $JLINE -ge 10 ]]; then
	JLINE=0
	echo "Sleeping"
	#break
	sleep 5
    fi

    # if [[ $ILINE -ge 5 ]]; then
    # 	echo $GROUP_ID
    # 	break
    # fi
done 

echo "Done"
