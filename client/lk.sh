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
target_vms=(02 06) # Example: kill programs on VM 02 and VM 03
for vm in "${target_vms[@]}"; do
  target_ip=$(printf "$base_ip" "$vm")
  echo "Sending kill command to $target_ip"
  
  ssh -o StrictHostKeyChecking=no "$target_ip" <<EOF
ps aux | grep "go run . [0-9]*" | grep -v grep | awk '{print \$2}' | xargs -r kill -9
EOF

done

echo "Script completed."
