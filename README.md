surrey-council-video-archive
============================

These are Python scripts that download [Surrey City Council video recordings](http://www.surrey.ca/city-government/6993.aspx) 
and re-uploads them onto YouTube.
The [Surrey City Council Meetings Auto-Upload channel on YouTube](https://www.youtube.com/channel/UCvDEI1KAPS5CjzDhsXa1jdw)
is automated using these scripts.

This project is maintained by [Carson Lam](https://www.carsonlam.ca) ([@carsonyl](https://twitter.com/carsonyl)).

The Git repository for this project is at https://github.com/rbcarson/surrey-council-video-archive.

Rationale
---------

Uploading Surrey City Council videos onto YouTube has benefits compared to 
the [City's official video archive](http://civic.neulion.com/cityofsurrey/):

* Videos are retained indefinitely, instead of a maximum of 2 years
* Improved video search and discovery
* Better user experience for video playback and seeking
* Playback is possible on platforms without Flash, such as smartphones and tablets
* Smaller video file sizes and more efficient use of bandwidth

Technical details
-----------------

This is a Python 3.4+ project that runs on all platforms. Python dependencies are listed in `requirements.txt`.
It requires [ffmpeg](https://ffmpeg.org/) and [AtomicParsley](https://bitbucket.org/wez/atomicparsley) to be on the path. 
Uploading to YouTube uses the [youtube-upload](https://github.com/tokland/youtube-upload) library.

Neulion, the service used by the City of Surrey for livestreams and video recordings, provides a Flash interface.
Behind the scenes, all videos are comprised of 2-second clips that the Flash player merges for playback.
This project includes code to discover available videos, download the corresponding ranges of 2-second clips, 
use ffmpeg to concatenate them into a single video, and then add appropriate metadata using AtomicParsley.
