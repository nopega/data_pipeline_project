#!/bin/bash
pwd
echo "Ready to clean the data?[1/0]"
read ready


if [ $ready -eq 1 ]
then 
    echo "Cleaning Data"
    python cleanse_data.py
    echo "Done cleaning data"
    cd ..
    cd file
    dev_version=$(head -n 1 changelog.md)
    echo "dev_version"$dev_version
    #directory="/Users/pongk/product_data_pipeline"
    #filename="$directory/changelog.md"


    #if [ ! -d "$directory" ]
    #then 
        #echo "Directory '$directory' does not exist. Creating it..."
        #mkdir -p "$directory"
        #echo "Directory '$directory' created successfully."
    #fi
    #if [ ! -e "$filename"]
    #then 
        #echo "File '$filename' does not exist. Creating it..."
        #touch "$filename"
        #echo "File '$filename' created successfully."
    #fi
    cd ..
    prod_version=$(head -n 1 product/changelog.md)
    echo "prod_version"$prod_version
    read -a splitversion_dev <<< $dev_version
    read -a splitversion_prod <<< $prod_version
    dev_version=${splitversion_dev[1]}
    prod_version=${splitversion_prod[1]}

    if [ $prod_version != $dev_version ]
    then
        echo "new changes detected.Do you want copy clean_file and missing_file to product_data_pipeline? [1/0]"
        read scriptcon
    else 
        scriptcon=0
    fi
else
    echo "come back when you ready"
fi

if [ $scriptcon -eq 1 ]
then 
    pwd
    cd file
    for filename in *
    do 
        echo "$filename"
        if [ $filename == "final_df.csv" ] || [ $filename == "final_df.parquet" ] || [ $filename == "miss_data.csv" ] || [ $filename == "miss_data.parquet" ]
        then 
            cp -r $filename C:\\Users\\pongk\\project_data_pipeline\\product
            echo "Copying" $filename
        fi
    done
else
    echo "Please come back when you are ready"
fi