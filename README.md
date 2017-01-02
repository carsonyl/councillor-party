Councillor Party
================

**Councillor Party** is a set of Python scripts for 
downloading city council videos hosted on Neulion and reuploading them onto YouTube.

The following YouTube channels are automated using Councillor Party:

* [Surrey BC City Council Meetings](https://www.youtube.com/channel/UCvDEI1KAPS5CjzDhsXa1jdw)
* [Burnaby BC City Council Meetings](https://www.youtube.com/channel/UCk7Xv8-7kPMzDrEEjJfU2Qw)
* [Vancouver BC City Council Meetings](https://www.youtube.com/channel/UCAOvmwJyHEGhV_vUYv82HxA)

This project is maintained by [Carson Lam](https://www.carsonlam.ca) ([@carsonyl](https://twitter.com/carsonyl)).

The Git repository for this project is at https://github.com/rbcarson/councillor-party.

Rationale
---------

Hosting these videos on YouTube brings benefits that can improve civic engagement and participation,
including:

* Videos are retained indefinitely, instead of a year or two
* Improved video search, discovery, and sharing
* Better user experience for video playback and seeking
* Playback is possible on platforms without Flash, such as smartphones and tablets
* Smaller video file sizes and more efficient use of bandwidth

Technical details
-----------------

This is a Python 3.4+ project that runs on all platforms. Python dependencies are listed in `requirements.txt`.
It requires [ffmpeg](https://ffmpeg.org/) to be on the path: `ffmpeg` and `ffprobe` in particular. 

Neulion serves its videos to browsers using a Flash player interface.
Behind the scenes, all videos are comprised of 2-second clips that the Flash player merges for playback.
This project includes code to discover available videos, download the corresponding ranges of 2-second clips, 
use ffmpeg to concatenate them into a single video, and re-upload them onto YouTube with configurable metadata.

Workflow
--------

A few steps need to be completed prior to first use:

1. Add a Google API Project and an OAuth2 client ID credential.
   Save the credential file file as `client_id.json`.
2. Define a configuration in `config.yaml`. Use the existing definitions as a template.
3. Run `youtube.py [config_id] authorize` to grant access to the YouTube channel to receive uploaded videos.
   The credentials are saved as `[config_id].client.json`.

The basic workflow for the download-transform-upload procedure is as follows:

1. Download the videos for a particular date using `download.py [config_id] [YYYY-MM-DD]`.
2. Concatenate and process the videos using `concat.py`.
3. Upload using `youtube.py [config_id] upload`.
