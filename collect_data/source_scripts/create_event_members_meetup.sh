COUNTRY_CODE=GB
CATEGORY=34

_QUERY="USE tier_0; SELECT distinct(member_id) FROM meetup_events_members;"
_CNF=/Users/$USER/Nesta/nesta_dataflow/db_config/tier-0.cnf
RESULTS=$(echo $_QUERY | mysql --defaults-extra-file=$_CNF)

FILE_COUNTER=0
rm event-member-meetup*_tier-0-inputs* &> /dev/null
for MEMBER_ID in $RESULTS; do
    if [[ $MEMBER_ID == "member_id" ]]; then
	continue
    fi
    #if grep -q $MEMBER_ID "done_files"; then
    #continue
    #fi
    EVENT_FILE_NAME=event-member-meetup${FILE_COUNTER}_tier-0-inputs
    echo -n meetup_event_member_$MEMBER_ID >> $EVENT_FILE_NAME
    NLINES=$(cat $EVENT_FILE_NAME | wc -l)
    if [[ $NLINES -lt 2000 ]];
    then
	echo "" >> $EVENT_FILE_NAME
    else
	# No new lines for done files	
	FILE_COUNTER=$((FILE_COUNTER+1))
    fi
done

python aws-tools/s3-create_files.py
