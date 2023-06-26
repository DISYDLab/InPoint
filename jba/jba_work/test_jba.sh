#!/bin/bash

#Define the string value
text="Welcome to LinuxHint"
BASE=$1

# Set space as the delimiter
IFS=' '
IFS=_

#Read the split words into an array based on space delimiter
read -a strarr <<< "$text"
read -a strarr2 <<< "$BASE"

#Count the total words
echo "There are ${#strarr[*]} words in the text."


# Print each value of the array by using the loop
for val in "${strarr[@]}";
do
  printf "$val\n"
done

# Print each value of the array by using the loop
for val in "${strarr2[@]}";
do
  printf "$val\n"
done
