RESULTS=$(echo "USE MarkovChainWebScrape; SELECT * FROM top_urls" | mysql --defaults-extra-file=/Users/hep/Nesta/nesta_dataflow/db_config/tier-0.cnf)
i=0
for url in $RESULTS;
do
    if [[ $i -eq 0 ]]; then
	i=$((i+1))
	continue
    fi

    if [[ $url == *.ac.uk* ]];
    then
	continue
    fi

    DONE=$(echo "USE tier_0; SELECT distinct(top_url) FROM all_university_urls where top_url = \"$url\"" | mysql --defaults-extra-file=/Users/hep/Nesta/nesta_dataflow/db_config/tier-0.cnf)
    SELENIUM=$(echo "USE MarkovChainWebScrape; SELECT url FROM selenium_urls where url = \"$url\"" | mysql --defaults-extra-file=/Users/hep/Nesta/nesta_dataflow/db_config/tier-0.cnf)
    WAIT_CONDITION=$(echo "USE MarkovChainWebScrape; SELECT wait_for FROM selenium_urls where url = \"$url\"" | mysql --defaults-extra-file=/Users/hep/Nesta/nesta_dataflow/db_config/tier-0.cnf)

    if [[ $DONE != "" ]];
    then
	echo "Skipping "$url
	continue
    fi

    if [[ $SELENIUM != "" ]];
    then
	SELENIUM="True"
	WAIT_CONDITION=($WAIT_CONDITION)
	WAIT_CONDITION=${WAIT_CONDITION[1]}
    else
	SELENIUM="False"
	WAIT_CONDITION=""
    fi
    
    echo "Running $url, $SELENIUM, $WAIT_CONDITION"
    sed -i .bak "s,top_url = TOP_URL,top_url = $url,g" config/uae_courses.config
    sed -i .bak "s,selenium = False,selenium = $SELENIUM,g" config/uae_courses.config
    python source_scripts/replace.py 'wait_condition = ' "wait_condition = $WAIT_CONDITION" config/uae_courses.config

    
    # if [[ $i -eq 3 ]];
    # then
    # 	echo "Waiting..."
    # 	wait
    # 	i=0	
    # fi

    gtimeout -s KILL 70m python collect_data.py --config uae_courses #&> logs/output-$i.out &	
    
    sed -i .bak "s,top_url = $url,top_url = TOP_URL,g" config/uae_courses.config
    sed -i .bak "s,selenium = $SELENIUM,selenium = False,g" config/uae_courses.config
    python source_scripts/replace.py "wait_condition = $WAIT_CONDITION" 'wait_condition = '  config/uae_courses.config

    echo -e "\n--------------------\n"
    i=$((i+1))    
done
