COUNTRY_CODE=GB
CATEGORY=34

_QUERY="USE tier_0; SELECT distinct(event_id) FROM meetup_groups_events WHERE group_id IN (SELECT id FROM meetup_groups WHERE country = '${COUNTRY_CODE}' AND category_id = ${CATEGORY}) AND event_id NOT IN (select distinct(event_id) from meetup_events_members);"
_CNF=/Users/$USER/Nesta/nesta_dataflow/db_config/tier-0.cnf
RESULTS=$(echo $_QUERY | mysql --defaults-extra-file=$_CNF)

FILE_COUNTER=0
rm event-meetup*_tier-0-inputs* &> /dev/null
for EVENT_ID in $RESULTS; do
    if [[ $EVENT_ID == "event_id" ]]; then
	continue
    fi
    if grep -q $EVENT_ID "done_files"; then
	continue
    fi
    EVENT_FILE_NAME=event-meetup${FILE_COUNTER}_tier-0-inputs
    echo -n event_meetup_$EVENT_ID >> $EVENT_FILE_NAME
    NLINES=$(cat $EVENT_FILE_NAME | wc -l)
    if [[ $NLINES -lt 500 ]];
    then
	echo "" >> $EVENT_FILE_NAME
    else
	# Delete the last line of the file
	FILE_COUNTER=$((FILE_COUNTER+1))
    fi
done

python aws-tools/s3-create_files.py
