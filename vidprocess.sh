#!/bin/bash

# Define the directory to search for video files
directory="/YOUR-ABSOLUTE/PATH/HERE"
user="username"

# Define list of all servers including localhost
servers=("localhost" "192.168.2.21" "192.168.2.11")
# Define a mapping of part files to server hostnames and IPs. Needed for monitor_progress 
declare -A server_map=(["aa"]="192.168.2.21" ["ab"]="192.168.2.11" ["ac"]="localhost")
part_files=("$directory/files_list_partaa" "$directory/files_list_partab" "$directory/files_list_partac") # Adjust as needed based on actual part files generated
# Adjust max_concurrent_jobs in process_files()

check_rw_access() { # Function to check read/write access. Called by check_directory_exists
    local server=$1
    local check_file="${directory}/rw_check.tmp"

    # Command to create and then remove a temporary file
    local cmd="touch '${check_file}' && rm '${check_file}'"

    if [ "$server" = "localhost" ]; then
        # Execute command directly for local server
        if ! eval "$cmd"; then
            echo "Error: Unable to write to ${directory} on local server. Check read/write permissions."
            exit 1
        fi
    else
        # Execute command via SSH for remote servers
        if ! ssh "$user"@"$server" "$cmd"; then
            echo "Error: Unable to write to ${directory} on server $server. Check read/write permissions."
            exit 1
        fi
    fi
}

check_directory_exists(){ # Check directory existence and read/write access on all servers
for server in "${servers[@]}"; do
    if [ "$server" = "localhost" ]; then
        # Check directory on the local server
        if [ ! -d "$directory" ]; then
            echo "Directory does not exist on local server."
            exit 1
        else
            echo "Directory exists on local server. Checking read/write access..."
            check_rw_access "$server"
        fi
    else
        # Check directory on remote servers
        if ! ssh "$user"@"$server" "[ -d '$directory' ]"; then
            echo "Directory does not exist on server $server."
            exit 1
        else
            echo "Directory exists on server $server. Checking read/write access..."
            check_rw_access "$server"
        fi
    fi
done
echo "All servers have read/write access to the directory. Proceeding with the script..."
}

find_files(){ #find all video files in $directory
# Find video files and generate a list
find "$directory" -type f -regex '.*\.\(mp4\|mov\|avi\|mkv\|flv\|wmv\|mpeg\|mpg\|webm\|3gp\|ogg\|ogv\|ts\)' > "$directory/files_list.txt"
echo "File list generated: $directory/files_list.txt"
}

prep_files() { #set up the error log and divide files_list for distribution to servers, based on file number. Last file will catch the remainder.
# Create or clear the error log file
error_log="$directory/error_log.txt"
echo "Error log: $error_log"
> "$error_log"

# Divide the total number of files evenly among the servers
total_files=$(wc -l < "$directory/files_list.txt")
echo "Total files: $total_files"
files_per_machine=$((total_files / 3))
echo "Files per machine: $files_per_machine"
files_per_thread=$((files_per_machine / 56))
echo "Files per thread: $files_per_thread"

# Split the file list into three parts
split -l "$files_per_machine" "$directory/files_list.txt" "$directory/files_list_part"

# Merge extra part file if exists
if [ -f "$directory/files_list_partad" ]; then
    cat "$directory/files_list_partac" "$directory/files_list_partad" > "$directory/temp_ac"
    mv "$directory/temp_ac" "$directory/files_list_partac"
    rm "$directory/files_list_partad"
fi
}

checkfiles() { # triple check that files_list.txt and all parts are valid paths before we start actually chewing on them

# After generating files_list.txt
echo "Checking validity of file paths in files_list.txt..."

while IFS= read -r file; do
    if [ ! -f "$file" ]; then
        echo "Invalid file path detected: $file"
        exit 1
    fi
done < "$directory/files_list.txt"

echo "All file paths in files_list.txt are valid."

for server in "${servers[@]}"; do
    for part_file in "${part_files[@]}"; do
        if [ "$server" = "localhost" ]; then
            # Check part file on the local server
            if [ ! -f "$part_file" ]; then
                echo "Part file does not exist on local server: $part_file"
                exit 1
            fi
        else
            # Check part file on remote servers
            if ! ssh "$user"@"$server" "[ -f '$part_file' ]"; then
                echo "Part file does not exist on server $server: $part_file"
                exit 1
            fi
        fi
    done
done

echo "All part files exist on all servers."
}

process_files() { # Function to process files in parallel with controlled concurrency. called by process_workers_start in loop
    local file_list_part="$1"
    local max_concurrent_jobs=10 # Adjust based on system capacity
    local job_count=0
    local base_marker_dir="$directory/zzzzz-process_markers"
    local part_name=$(basename "$file_list_part" | sed 's/files_list_part//') # Extract 'aa', 'ab', or 'ac'
    local marker_dir="$base_marker_dir/$part_name"
    local transcoded_dir="$directory/transcoded" # Subdirectory for transcoded files

    # Ensure the marker and transcoded directory exists
    mkdir -p "$marker_dir"
	mkdir -p "$transcoded_dir"

    echo "Inside process_files with: $file_list_part on host: $(hostname)"

    declare -a job_pids=()  # Array to hold job PIDs

    while IFS= read -r file; do
	local out_file="$transcoded_dir/$(basename "$file")"
        if (( job_count >= max_concurrent_jobs )); then
            # Wait for any job to finish before starting a new one
            wait -n "${job_pids[@]}"
            # Remove finished jobs from the array
            job_pids=($(jobs -rp))
            job_count=${#job_pids[@]}
        fi

        (
           # echo "Processing file on host: $(hostname)"
            if ffmpeg -nostdin -v error -i "$file" \
                -vf "crop=1584:1072:158:6,scale=640:480" \
                -c:v libx264 -preset medium -crf 23 \
                -c:a aac -ar 48000 -ac 2 -b:a 128k \
                "$out_file" -y; then
                # On success, create a success marker file
                touch "$marker_dir/$(basename "$file").success"
                # echo "Successfully processed $file on host: $(hostname)"
            else
		#echo "Error log: $error_log"
                echo "$file" >> "$error_log"
		touch "$marker_dir/$(basename "$file").failure"
            fi
        ) &

        # Capture PID of the background job
        job_pids+=($!)
        ((job_count++))
    done < "$file_list_part"

    # Wait for all remaining background jobs to finish
    wait

    # count and log the number of success/failure markers for this part after processing
    local success_count=$(find "$marker_dir" -type f -name '*.success' | wc -l)
    local failure_count=$(find "$marker_dir" -type f -name '*.failure' | wc -l)
    local processed_count=$((success_count + failure_count))
    echo "Total files successfully processed for part $part_name: $processed_count"
}

monitor_progress() { # monitor job progress, called in process_workers_start
    local base_marker_dir="./zzzzz-process_markers"
    local report_interval=600 # Report every 10 minutes (600 seconds)
    local total_processed

while :; do
        echo "---- Processing Report: $(date) ----"
        total_processed=0
        for part in "${!server_map[@]}"; do
            local server_ip=${server_map[$part]}
            local hostname

            # Fetch hostname for remote servers, or use $(hostname) for localhost
            if [[ "$server_ip" == "localhost" ]]; then
                hostname=$(hostname)
            else
                hostname=$(ssh "$server_ip" 'hostname')
            fi

            local part_dir="$base_marker_dir/$part"
            local count=0
            if [[ -d "$part_dir" ]]; then
                count=$(find "$part_dir" -type f -name '*.success' | wc -l)
            fi

            echo "Server: $hostname ($server_ip), Part $part: $count files processed."
            ((total_processed+=count))
        done
        echo "Total processed across all servers: $total_processed"
        echo "----------------------------------------"

        sleep "$report_interval"
    done
}

process_workers_start() { # spawn workers and start processing the files.
monitor_progress &
monitor_pid=$!

# Initialize an array to hold background process IDs
bg_processes=()

# Process files on each server
for part in aa ab ac; do
    file_list_part="$directory/files_list_part$part"
    base_marker_dir="$directory/zzzzz-process_markers" # Ensure this uses the global $directory variable

    if [ "$part" = "aa" ]; then
        server="192.168.2.21"
    elif [ "$part" = "ab" ]; then
        server="192.168.2.11"
    else
        server="localhost"
    fi

    if [ "$server" != "localhost" ]; then
        # Use SSH to process files remotely in parallel
        ssh "$user"@"$server" "bash -c 'directory=\"$directory\"; file_list_part=\"$file_list_part\"; base_marker_dir=\"$base_marker_dir\"; error_log=\"$error_log\"; $(declare -f process_files); process_files \"\$file_list_part\"'" &
        bg_processes+=($!)
    else
        # Process files locally in parallel
        process_files "$file_list_part" &
        bg_processes+=($!)
    fi
done

# Wait for all background processes to complete
for pid in "${bg_processes[@]}"; do
    wait $pid
done

echo "All processing complete."
echo "Error log is located at: $error_log"
kill "$monitor_pid"

}

summarize_and_compare() { # summarize completed work, check the initial work list. If different, missed some files. Write missed files to errormissing.log
    local base_marker_dir="$directory/zzzzz-process_markers"
    local processed_files_log="$directory/errormissing.log"
    local total_requested_files=$(wc -l < "$directory/files_list.txt")
    local total_processed_files=0

    # Reset or create the log file
    > "$processed_files_log"

    # Generate a temporary file listing all processed filenames (without the .success extension)
    local processed_filenames="$directory/processed_filenames.tmp"
    find "$base_marker_dir" -type f -name '*.success' -exec basename {} .success \; > "$processed_filenames"

    echo "Summary of processing:"
    for part in aa ab ac; do
    # Count both success and failure markers within each part
    local success_count=$(find "$base_marker_dir/$part" -type f -name '*.success' | wc -l)
    local failure_count=$(find "$base_marker_dir/$part" -type f -name '*.failure' | wc -l)
    local part_processed_count=$((success_count + failure_count))

    echo "Part $part processed files: $part_processed_count"
    ((total_processed_files+=part_processed_count))
    done

    echo "Total requested files: $total_requested_files"
    echo "Total processed files: $total_processed_files"

    # Directly log missed file paths to errormissing.log without additional comments
    if [ "$total_requested_files" -ne "$total_processed_files" ]; then
        echo "Identifying missed files..."

        while IFS= read -r file; do
            local file_basename=$(basename "$file")
            if ! grep -Fxq "$file_basename" "$processed_filenames"; then
                echo "$file" >> "$processed_files_log"
            fi
        done < "$directory/files_list.txt"
    else
        echo "All requested files have been successfully processed."
		rm -f "$processed_files_log" # delete the errormissing.log file, it's blank
    fi
	# Cleanup temporary file
    rm "$processed_filenames"

#reprocessing stage
	if [ -f "$processed_files_log" ]; then
		echo "Missed files detected. Reprocessing needed."
		
		while true; do
        # Prompt the user for their decision on reprocessing
        echo "Would you like to reprocess the missed files now? (y/n)"
        read -r user_decision
        
        if [[ "$user_decision" == "y" ]]; then
            echo "Reprocessing missed files......"
            
			cp "$processed_files_log" "$directory/files_list.txt" # clears files_list.txt and overwrites with the contents of errormissing.log 
			rm -rf "$directory/zzzzz-process_markers" # delete the old success files. If we dont, then on the second round through if we have any leftovers theyll trigger summarize_and_compare for missing files. 
			prep_files #divide files_list.txt for distribution to servers, based on file number. Last file will catch the remainder.
			checkfiles # triple check that files_list.txt and all parts are valid paths before we start actually chewing on them. useful to debug cryptic "no such file" errors
			process_workers_start # spawn workers and start processing the files.
			# the potential exists for summarize_and_compare to call itself below. These nested loops should all self-terminate if the user or script chooses not to reprocess.
			summarize_and_compare # summarize completed work, check the initial work list. If different, missed some files. Write missed files to errormissing.log. 
            
            break  # Exit the loop after handling valid input
        elif [[ "$user_decision" == "n" ]]; then
            echo "User chose not to reprocess missed files. Proceeding without reprocessing."
            break  # Exit the loop after handling valid input
        else
            echo "Invalid input. Please enter 'y' for yes or 'n' for no."
            # The loop will continue, prompting the user again for valid input
        fi
    done
		
	else
		echo "No missed files. Reprocessing not required. Script exiting."
	fi
}


#main program. Function-heavy because it's easier to debug, so the following statements are functions meant to run sequentially.

check_directory_exists # Check directory existence and read/write access on all servers
find_files # find all video files in $directory
# re-run from here
prep_files #divide files_list for distribution to servers, based on file number. Last file will catch the remainder.
# checkfiles # triple check that files_list.txt and all parts are valid paths before we start actually chewing on them. useful to debug cryptic "no such file" errors
process_workers_start # spawn workers and start processing the files.
summarize_and_compare # summarize completed work, check the initial work list. If different, missed some files. Write missed files to errormissing.log

#sort the error_log.txt file alphabetically for ease of review
sort "$error_log" | uniq > "$error_log.tmp" && mv "$error_log.tmp" "$error_log"

# Clean up temporary files. not a function because we should only clean up if the script made it all the way through to here.
echo "Cleaning up temporary files and exiting"
rm "$directory/files_list.txt" "$directory/files_list_part"*
rm -rf "$directory/zzzzz-process_markers"
