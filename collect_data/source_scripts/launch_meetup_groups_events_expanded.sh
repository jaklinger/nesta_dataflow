PREFIX=meet_groups_events_expanded_

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

# _QUERY='CREATE TEMPORARY TABLE start_groups (                          
# 	      select id from meetup_groups
# 	      where country_name="United Kingdom" and category_id=34);

# 	      CREATE TEMPORARY TABLE start_events (
# 	      select distinct(event_id) from meetup_groups_events
# 	      where group_id in (select id from start_groups));
	      
# 	      CREATE TEMPORARY TABLE start_members (                         
# 	      select distinct(member_id) from meetup_events_members          
# 	      where event_id in (select event_id from start_events));

# 	      CREATE TEMPORARY TABLE expanded_groups (                       
# 	      select group_id,group_urlname from meetup_groups_members       
# 	      where member_id in (select member_id from start_members));

# 	      select concat(group_urlname,"_",group_id) from expanded_groups where group_id not in (select group_id from meetup_groups_events) group by group_id, group_urlname;'

_QUERY='select concat(group_urlname,"_",group_id) from meetup_groups_members where group_id in (select id from meetup_groups where country_name="United Kingdom" and category_id=34) and group_id not in (select group_id from meetup_groups_events);'

RESULTS=$(echo $_QUERY | mysql --defaults-extra-file=/Users/$USER/Nesta/nesta_dataflow/db_config/tier-0.cnf)


ILINE=0
JLINE=0
for GROUP_ID in $RESULTS; do
    ILINE=$((ILINE+1))
    JLINE=$((JLINE+1))
    if [[ $ILINE -le 2 ]]; then
        continue
    fi

    #echo $GROUP_ID
    LAUNCH $GROUP_ID &
    #aws s3 rm s3://tier-0/meetup_$GROUP_ID --quiet &
    if [[ $JLINE -ge 30 ]]; then
        JLINE=0
        echo "Sleeping"
        sleep 5
    fi

    # if [[ $ILINE -ge 5 ]]; then
    #   echo $GROUP_ID
    #   break
    # fi
done

echo "Done"
