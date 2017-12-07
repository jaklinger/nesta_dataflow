COUNTRY="United Kingdom"
CATEGORY=34
COUNTRY_CODE="GB"

# Country --> Groups
#python collect_data.py --config meetup_country_groups --country $COUNTRY --category $CATEGORY
#sleep 10
# Country groups --> Members
python collect_data.py --config meetup_groups_members --country $COUNTRY --category $CATEGORY
#sleep 10
# Members --> All groups
#source source_scripts/launch_meetup_members.sh $COUNTRY $COUNTRY_CODE $CATEGORY 
#python collect_data.py --config meetup_members_awsreduce --country $COUNTRY --category $CATEGORY
#sleep 30
# All groups --> Group details
#source source_scripts/launch_meetup_groups.sh $COUNTRY $COUNTRY_CODE $CATEGORY
#python collect_data.py --config meetup_group_details_awsreduce --country $COUNTRY --category $CATEGORY
