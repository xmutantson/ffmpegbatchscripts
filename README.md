This repository contains a script that was painstakingly developed to automate and batch process ffmpeg jobs across various servers. There are many ways to formulate an ffmpeg command, this example rescales a letterboxed image captured in a 1920x1080 frame to 640x480. The script first defines all of the functions, then calls them all in sequence. This is needed later to repeat a run if for some reason the process misses some files.

The following assumptions are made about this script:

* There are 3 servers available over SSH
* All SSH sessions are using the same username and passwordless auth (pubkey in my case)
* Those servers all have the working directory mounted in the same place. I used a network share.
* Each server has r/w access to the working directory
* One of the processing servers is localhost
* All servers are of the same capability, can handle the same concurrency level (10 is used here, each of my servers has 56 threads)
* All servers have ffmpeg installed and available
  

The script wil create:
* Temprary files in the working directory. Read the code for more info, all of them clean themselves up
* error_log.txt (used to collect ffmpeg errors/failures to read file)
  

Don't worry about:

* wait: no such job (means processing exited some time before wait was called. Job is successful though
* Evenly splitting files: The script splits on number of files, not size of files. This can mean that things are a teensy bit skewed towards one server or another. If you want everything to be exactly the same, feel free. It doesn't make much of a difference in my case and I was lazy so I didn't handle the file sizes and split that way.

