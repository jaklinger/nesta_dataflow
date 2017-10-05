#!/bin/bash

#---- My AWS settings
MYID="377767275214"
REGION="eu-west-1"
PROFILE="default"
ROLE="nesta-innovation-mapping"
BUCKET="nesta-datapipeline"
alias aws=/Users/hep/.local/bin/aws

#---- Default values
DESCRIPTION="No description given"
MODE="INVOKE"
REFRESH="NO"
REFRESH_ENV="NO"
DRYRUN="NO"

#---- Required values
SCRIPT="REQUIRED"
SCRIPTPATH="REQUIRED"

#---- Command line arguments
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
	-d|--description)
	    DESCRIPTION="$2"
	    shift # past argument
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

#---- Shame the user for not providing key parameters
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

#---- My settings
FUNCTION_NAME="DataPipeline-${SCRIPT}"
HANDLER="${SCRIPT}.run"
DESCRIPTION="${DESCRIPTION}"

#---- My organisation's settings
TIMEOUT=300
MEMORY_SIZE=512
ROLE="arn:aws:iam::${MYID}:role/${ROLE}"

#---- My lambda schedule settings
SCHEDULE_EXPR="rate(5 minutes)"
SCHEDULE_RULE=$FUNCTION_NAME"-Schedule"
SCHEDULE_RULE_ID=1
SOURCE_ARN="arn:aws:events:$REGION:$MYID:rule/$SCHEDULE_RULE"
TARGETS="{\"Id\" : \"$SCHEDULE_RULE_ID\", \"Arn\": \"arn:aws:lambda:$REGION:$MYID:function:$FUNCTION_NAME\"}"

#---- My IO/OS settings
OUTLOG="outputfile.txt"
TOPDIR="$PWD"
ZIPFILE="DataPipeline-${SCRIPT}.zip"
ENVDIR="$TOPDIR/env"
ENVZIP="${SCRIPT}.zip"    

#__________________________
#---- Prepare the zip file
function PREPARE_ZIP {
    echo "Zipping up files"
    
    cd $TOPDIR
    cp $ENVDIR/$ENVZIP $TOPDIR/$ZIPFILE
    
    cd $TOPDIR/$SCRIPTPATH 
    zip -g $TOPDIR/$ZIPFILE *py &> /dev/null 
    cd $TOPDIR 
}

#__________________________
#---- Delete the function
function DELETE_FUNCTION {
    echo "Deleting function "$FUNCTION_NAME &&
    aws lambda delete-function \
	--function-name $FUNCTION_NAME \
	--region $REGION \
	--profile $PROFILE
}

#__________________________
#---- Create the function
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
#---- Invoke function according to a schedule
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
#---- Invoke the function
function INVOKE_FUNCTION {
    echo "Invoking "$FUNCTION_NAME &&
    aws lambda invoke \
	--invocation-type RequestResponse \
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
#---- Invoke function according to a schedule
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
    
    # Create and run the docker
    echo "Running the container build..."
    docker run -v $(pwd):/outputs --name=$CONTAINER_NAME -t amazonlinux /bin/bash /outputs/build.sh
    
    cd $TOPDIR
    mv $SCRIPTPATH/venv.zip $ENVDIR/$ENVZIP    
    rm $SCRIPTPATH/full-venv.zip 
    
    # # Create the environment if it doesn't already exist
    # echo "Attempting to create a conda environment called $SCRIPT..."
    # conda create python=3.6 --name $SCRIPT &> _RESULT
    # RESULT=$(cat _RESULT)
    # rm _RESULT    
    # if [[ $RESULT == *"prefix already exists"* ]];
    # then
    # 	echo "Environment $SCRIPT already exists"
    # else
    #     echo "Created environment $SCRIPT"
    # fi
    
    # # Activate the environment
    # source activate $SCRIPT

    # # Generate a requirements file
    # pip install pipreqs &> /dev/null
    # pipreqs --force $TOPDIR/$SCRIPTPATH
    # # Use the requirements to build an environment
    # while read PKGNAME; do
    # 	_PKGNAME=$(echo $PKGNAME | awk -F== '{print $1}')
    # 	echo "Installing "$_PKGNAME
    # 	yes | pip install $PKGNAME
    # 	# conda install --yes $_PKGNAME &> _RESULT
    # 	# RESULT=$(cat _RESULT)
    # 	# rm _RESULT
    # 	# if [[ $RESULT == *"PackageNotFoundError"* ]];
    # 	# then
    # 	#     echo "Couldn't find the package in conda"
    # 	#     echo "Trying pip..."
    # 	#     yes | pip install $PKGNAME
    # 	# fi	
    # done < $TOPDIR/$SCRIPTPATH/requirements.txt
    # rm $TOPDIR/$SCRIPTPATH/requirements.txt    

    # pip install numpy
    
    # # Zip up the environment
    # PYTHONBIN=$(echo $PATH | cut -d: -f1)
    # PYTHONLIB=$(ls -d ${PYTHONBIN}/../lib/python*/site-packages/)
    # cd $PYTHONLIB
    # zip -r $ENVDIR/$ENVZIP * &> /dev/null
}

function DEPLOY_ZIP(){
    echo "Deploying new environment"
    aws s3 cp $TOPDIR/$ZIPFILE s3://$BUCKET --profile $PROFILE || return 1
    rm $TOPDIR/$ZIPFILE    
}

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
