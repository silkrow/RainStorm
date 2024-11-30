#!/bin/bash

# Remove all files in the client folder
rm -f client/*

# Copy files from business folder to client folder for files named business_1.txt to business_20.txt
cp business/business_{1..20}.txt client/

# Remove all files in the server folder
rm -f server/*

