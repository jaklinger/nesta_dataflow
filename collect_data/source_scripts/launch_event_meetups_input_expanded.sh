function LAUNCH(){
    EVENT_ID=$1
    touch event_meetup_$EVENT_ID
    RESPONSE=$(aws s3 cp event_meetup_$EVENT_ID s3://tier-0-inputs --profile default)
    rm event_meetup_$EVENT_ID &> /dev/null
    if [[ $RESPONSE == *failed* ]];
    then
        echo $RESPONSE
        echo event_meetup_$EVENT_ID >> failed_tasks
    else
        echo $EVENT_ID" submitted"
    fi
}

_QUERY="SELECT distinct(event_id) FROM meetup_groups_events WHERE event_id NOT IN (SELECT distinct(event_id) FROM meetup_events_members) and group_id in (select id as group_id from meetup_groups where category_id = 2 and country = 'GB');"
_CNF=/Users/$USER/Nesta/nesta_dataflow/db_config/tier-0.cnf
RESULTS=$(echo $_QUERY | mysql --defaults-extra-file=$_CNF)

BASH_ID=$$
ILINE=0 # Global counter
for EVENT_ID in $RESULTS; do
    ILINE=$((ILINE+1))
    if [[ $ILINE -le 1 ]]; then
        continue
    fi

    # Launch event
    LAUNCH $EVENT_ID &

    # If too many processes
    CHILDREN=`ps -eo ppid | grep -w $BASH_ID`
    NUM_CHILDREN=`echo $CHILDREN | wc -w`
    if [[ $NUM_CHILDREN -ge 10 ]]; then
	echo "=> Waiting for any of $NUM_CHILDREN processes"
	sleep 3
    fi    
done

echo "Done"
