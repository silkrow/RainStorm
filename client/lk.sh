#!/bin/bash

# Variables
base_ip="fa24-cs425-68%02d.cs.illinois.edu"

go_program_command="go run RainStorm.go app1_1 app1_2 10000.csv f2 3"
# Launch Go program locally
eval "$go_program_command &"  # Start the Go program in the background
main_pid=$!
echo "Go program started with PID $main_pid on $(hostname)"
# Wait 1.5 seconds
sleep 1.5

# Choose two VMs to send the kill command
target_vms=(02 06) # Example: send SIGINT to Go program on VM 02 and VM 06
for vm in "${target_vms[@]}"; do
  target_ip=$(printf "$base_ip" "$vm")
  echo "Simulating Ctrl+C (SIGINT) on Go program running on $target_ip"

  # SSH to the remote VM and simulate Ctrl+C (SIGINT)
  ssh -o StrictHostKeyChecking=no "$target_ip" <<EOF
  # Get the PID of the 'go run' process
  pid=\$(pgrep -f "screen")
  
  # If a 'go run' process is found, simulate Ctrl+C by sending SIGINT
  if [ -n "\$pid" ]; then
    echo "Sending SIGINT (Ctrl+C) to PID \$pid"
    kill -2 \$pid  # Send SIGINT (Ctrl+C) to the process
  else
    echo "No 'go run' process found."
  fi
EOF
done

echo "Script completed."
