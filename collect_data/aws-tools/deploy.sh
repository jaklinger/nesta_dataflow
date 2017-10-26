#!/bin/bash

# My AWS settings
MYID="377767275214"
REGION="eu-west-1"
PROFILE="default"
ROLE="nesta-innovation-mapping"
BUCKET="nesta-datapipeline"
alias aws=/Users/hep/.local/bin/aws

# Default values
CONFIG=""
DESCRIPTION="No description given"
MODE="INVOKE"
REFRESH="NO"
REFRESH_ENV="NO"
DRYRUN="NO"

# Required values
SCRIPT="REQUIRED"
SCRIPTPATH="REQUIRED"

# Command line arguments
POSITIONAL=()
while [[ $# -gt 0 ]]
do
    key="$1"

    case $key in
	-s|--script)
	    SCRIPT="$2"
	    shift # past argument
	    shift # past value
	    ;;
	-sp|--scriptpath)
	    SCRIPTPATH="$2"
	    shift # past argument
	    shift # past value
	    ;;
	-c|--config)
	    CONFIG="$2"
	    shift # past argument
	    shift # past value
	    ;;
	-d|--description)
	    DESCRIPTION="$2"
	    shift # past argument
	    shift # past value	    
	    ;;
	-r|--refresh)
	    REFRESH="YES"
	    shift # past argument
	    ;;
	-re|--refreshenv)
	    REFRESH_ENV="YES"
	    shift # past argument
	    ;;
	-sc|--schedule)
	    MODE="SCHEDULE"
	    shift # past argument
	    ;;
	-dr|--dryrun)
	    DRYRUN="YES"
	    shift # past argument
	    ;;	
	*)    # unknown option
	    POSITIONAL+=("$1") # save it in an array for later
	    shift # past argument
	    ;;
    esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters     

# Shame the user for not providing key parameters
if [[ $SCRIPTPATH == "REQUIRED" ]];
then
    if [[ $REFRESH == "YES" ]];
    then
	echo -e "Error:\t --scriptpath parameter not provided, but --refresh was specified."
	return 1
    fi
fi
if [[ $SCRIPT == "REQUIRED" ]];
then
    echo -e "Error:\t --script parameter not provided"
    return 1
fi

# My settings
FUNCTION_NAME="DataPipeline-${SCRIPT}"
HANDLER="${SCRIPT}.run"
DESCRIPTION="${DESCRIPTION}"

# My organisation's settings
TIMEOUT=300
MEMORY_SIZE=512
ROLE="arn:aws:iam::${MYID}:role/${ROLE}"

# My lambda schedule settings
SCHEDULE_EXPR="rate(5 minutes)"
SCHEDULE_RULE=$FUNCTION_NAME"-Schedule"
SCHEDULE_RULE_ID=1
SOURCE_ARN="arn:aws:events:$REGION:$MYID:rule/$SCHEDULE_RULE"
TARGETS="{\"Id\" : \"$SCHEDULE_RULE_ID\", \"Arn\": \"arn:aws:lambda:$REGION:$MYID:function:$FUNCTION_NAME\"}"

# My IO/OS settings
OUTLOG="outputfile.txt"
TOPDIR="$PWD"
ZIPFILE="DataPipeline-${SCRIPT}.zip"
ENVDIR="$TOPDIR/env"
ENVZIP="${SCRIPT}.zip"    

#__________________________
# Prepare the zip file
function PREPARE_ZIP {
    echo "Zipping up files"
    
    cd $TOPDIR
    cp $ENVDIR/$ENVZIP $TOPDIR/$ZIPFILE
    
    cd $TOPDIR/$SCRIPTPATH 
    zip -g $TOPDIR/$ZIPFILE *py &> /dev/null 
    cd $TOPDIR 
}

#__________________________
# Delete the function
function DELETE_FUNCTION {
    echo "Deleting function "$FUNCTION_NAME &&
    aws lambda delete-function \
	--function-name $FUNCTION_NAME \
	--region $REGION \
	--profile $PROFILE
}

#__________________________
# Create the function
function CREATE_FUNCTION {
    echo "Creating function "$FUNCTION_NAME
    aws lambda create-function \
	--region $REGION \
	--function-name $FUNCTION_NAME \
	--code S3Bucket=$BUCKET,S3Key=$ZIPFILE \
	--role $ROLE \
	--handler $HANDLER \
	--runtime python3.6 \
	--description "$DESCRIPTION" \
	--timeout $TIMEOUT \
	--profile $PROFILE \
	--memory-size $MEMORY_SIZE
}

#__________________________
# Invoke function according to a schedule
function FUNCTION_EXISTS {
    echo "Checking whether "$FUNCTION_NAME" exists"
    aws lambda get-function \
	--function-name $FUNCTION_NAME &> _RESPONSE
    # Transfer the file contents to a variable
    RESPONSE=$(cat _RESPONSE)
    rm _RESPONSE
    # Then evaluate the status
    [[ $RESPONSE != *ResourceNotFoundException* ]];
}

#__________________________
# Invoke the function
function INVOKE_FUNCTION {
    echo "Invoking "$FUNCTION_NAME &&
    aws lambda invoke \
	--invocation-type Event \
	--region $REGION \
	--function-name $FUNCTION_NAME \
	--log-type Tail \
	--profile $PROFILE \
	$OUTLOG &> TMPOUT &&
    python utils/aws/decode_lambda_output.py $(cat TMPOUT) &&
    echo "Output from the handler:" &&
    cat $OUTLOG &&
    echo "" &&
    rm $OUTLOG    
    rm TMPOUT
}

#__________________________
# Invoke function according to a schedule
function SCHEDULE_FUNCTION {
    echo "Adding permissions to function "$FUNCTION_NAME &&
    aws lambda add-permission \
	--function-name $FUNCTION_NAME \
	--statement-id $MYID \
	--action 'lambda:InvokeFunction' \
	--principal events.amazonaws.com \
	--source-arn $SOURCE_ARN &> /dev/null || echo -e "\t==> Couldn't add this permission (has it already been added?)" &&
    echo "Removing targets "$SCHEDULE_RULE_ID" from the rule "$SCHEDULE_RULE &&
    aws events remove-targets \
	--rule $SCHEDULE_RULE \
	--ids $SCHEDULE_RULE_ID &&
    echo "Deleting rule "$SCHEDULE_RULE &&
    aws events delete-rule \
	--name $SCHEDULE_RULE &&
    echo "Adding expression "$SCHEDULE_EXPR" to new rule "$SCHEDULE_RULE &&
    aws events put-rule \
	--name $SCHEDULE_RULE \
	--schedule-expression "$SCHEDULE_EXPR" &&
    echo "Invoking schedule "$SCHEDULE_RULE &&
    aws events put-targets \
	--rule $SCHEDULE_RULE \
	--targets "$TARGETS"
}

#__________________________
# Create AWS environment from docker image
function CREATE_ENVIRONMENT(){

    CONTAINER_NAME="tmp_docker"

    # Check if the container exists yet
    CONTAINER=$(docker ps -aqf "name=$CONTAINER_NAME")
    if [[ $CONTAINER != "" ]];
    then
	echo "Removing container "$CONTAINER_NAME
	docker rm -f $CONTAINER &> /dev/null
    fi

    # Copy the build script
    cp $TOPDIR/aws-tools/build.sh $SCRIPTPATH
    cd $SCRIPTPATH

    # If there are config files, apply before/after replacements
    BEFORE=""
    echo $CONFIG
    if [[ $CONFIG != "" ]];
    then
	echo "Copying precommands"
	cp $TOPDIR/$CONFIG/before.sh . &> /dev/null
	cp $TOPDIR/$CONFIG/after.sh . &> /dev/null
    fi
    
    # Create and run the docker
    echo "Running the container build..."
    docker run -v $(pwd):/outputs --name=$CONTAINER_NAME -t amazonlinux /bin/bash /outputs/build.sh
    
    cd $TOPDIR
    mv $SCRIPTPATH/venv.zip $ENVDIR/$ENVZIP    
    rm $SCRIPTPATH/full-venv.zip 
    rm $SCRIPTPATH/build.sh
    rm $SCRIPTPATH/before.sh &> /dev/null
    rm $SCRIPTPATH/after.sh &> /dev/null
}


#__________________________
# Deploy the environment to S3
function DEPLOY_ZIP(){
    # Check the file size
    TOTAL_SIZE=$(unzip -l $TOPDIR/$ZIPFILE | tail -n1 | head -n1 | awk '{print $1;}')
    if [[ $TOTAL_SIZE -gt 262144000 ]];
    then
	echo -e "Error:\tZip file exceeds AWS Lambda restrictions";
	return 1
    fi
    # Deploy
    echo "Deploying new environment"
    aws s3 cp $TOPDIR/$ZIPFILE s3://$BUCKET --profile $PROFILE || return 1
    rm $TOPDIR/$ZIPFILE
}

#__________________________
# MAIN

# If function needs to be recreated
FUNCTION_EXISTS
FRESULT=$?
if [ $FRESULT -ne 0 ] || [[ $REFRESH == "YES" ]];
then
    # Check whether the function exists first
    if [ $FRESULT -eq 0 ];
    then
	DELETE_FUNCTION || return	
    fi
    FRESULT="NULL"
    
    # Prepare the zip file and create the function
    ls $ENVDIR/$ENVZIP &> /dev/null
    if [ $? -ne 0 ] || [[ $REFRESH_ENV == "YES" ]];
    then
	CREATE_ENVIRONMENT || return
    fi
    PREPARE_ZIP || return
    DEPLOY_ZIP || return
    CREATE_FUNCTION || return    
fi

# Invoke or schedule
if [[ $DRYRUN == "NO" ]];
then
    # Check whether the function exists first
    FUNCTION_EXISTS
    if [ $? -ne 0 ] || [[ "$FRESULT" != "NULL" ]];
    then
	echo "Function "$FUNCTION_NAME" not found!"
	return 1
    fi

    # If it exists, invoke it
    if [[ $MODE == "INVOKE" ]];
    then
	INVOKE_FUNCTION
    else
	SCHEDULE_FUNCTION 
    fi
fi
