#!/bin/bash

# Define the port number
PORT=5555

# Step 1: Find the process using the port
PID=$(sudo lsof -t -i :$PORT)

# Check if a process is found
if [ -n "$PID" ]; then
    echo "Found process using port $PORT: PID $PID"

    # Step 2: Kill the process
    echo "Killing process $PID..."
    sudo kill -9 $PID
    echo "Process $PID killed."
else
    echo "No process found using port $PORT."
fi
