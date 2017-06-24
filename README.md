Councillor Party
================

**Councillor Party** is a set of Python scripts for downloading city council videos hosted by various vendors,
and then re-uploading them onto YouTube and S3. Supported vendors include Neulion, InsInc, and Granicus.

The following YouTube channels are automated using Councillor Party:

* [Coquitlam BC City Council Meetings](https://www.youtube.com/channel/UCMvE5ag8fWAoFxLbw62D9rw) (InsInc)
* [Surrey BC City Council Meetings](https://www.youtube.com/channel/UCvDEI1KAPS5CjzDhsXa1jdw) (Granicus and Neulion)
* [Burnaby BC City Council Meetings](https://www.youtube.com/channel/UCk7Xv8-7kPMzDrEEjJfU2Qw) (Neulion)
* [Vancouver BC City Council Meetings](https://www.youtube.com/channel/UCAOvmwJyHEGhV_vUYv82HxA) (Neulion)
* [Langley BC City Council Meetings](https://www.youtube.com/channel/UCdUnSBiupuVc5HynLUWWlxQ) (InsInc and Neulion)

This project is maintained by [Carson Lam](https://www.carsonlam.ca) ([@carsonyl](https://twitter.com/carsonyl)).

The Git repository for this project is at https://github.com/carsonyl/councillor-party.

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

Neulion and Granicus use a Flash player to play videos that are served in small pieces, each a few seconds long.
Councillor Party gathers these pieces and concatenates them into a single video.

InsInc uses a Silverlight player to play videos served as a Windows Media stream.
Councillor Party downloads the whole stream and then splices out video segments according to clip timestamps.
A single stream may contain more than one meeting.

Workflow
--------

The basic workflow for the download-transform-upload procedure is as follows:

1. Download the metadata for a given date using `councillor-party.py [config_id] metadata [YYYY-MM-DD]`.
2. Download the videos for a given date using `councillor-party.py [config_id] download [YYYY-MM-DD]`.
3. Perform any required processing (concatenation, splicing, etc.) using `councillor-party.py [config_id] process`.
4. Upload the processed video, with assembled metadata, using `councillor-party.py [config_id] youtube upload`.
5. Upload to an Amazon S3 bucket for archival purposes, using `councillor-party.py [config_id] s3`.

In order to upload videos to YouTube, additional setup is needed:

1. Add a Google API Project and an OAuth2 client ID credential.
   Save the credential file under `auth/client_id.json`.
2. Define a configuration in `config.yaml`. Use the existing definitions as a template.
3. Run `councillor-party.py [config_id] youtube authorize`
   to grant access to the YouTube channel to receive uploaded videos.
   The credentials are saved under `auth/[config_id].token.json`.

Due to various quirks and errors that may be present in meeting timecodes and clip names,
this process sometimes requires babysitting. Temporary code changes or debugger interventions
may be needed to correctly handle certain videos.
