# nbadb (v2)

This is a complete overhaul of [alberlyu's nbadb project](https://github.com/albertlyu/nbadb). At this point, most of the original code has been rewritten, and the original project serves more as an idea than something to build on.

The code is being rewritten to be:

### More Useable
...By fetching data by date, and then by game_id, instead of by resource type

### Faster
...By using a pool of worker threads and by aggregating many INSERTS into bulk insert statements. For loading small chunks of data, the new version of the code is roughly 10x faster. Performance testing is still being done.

### More Robust
The code was barely functional when I started. My goal is to be able to scrape every morsel of information about the NBA, with or without schemas or structure.


## Overview

Currently there are three flavors of the ```staging``` script.

All three take the same arguments in these three ways

1. ```python staging.py``` will load data for yesterday's games
2. ```python staging.py 2014-12-25``` will load data for only December 25, 2014
3. ```python staging.py 2014-12-25 2015-12-1``` will load data from December 25, 2014 to December 1, 2015

The three flavors differ in their efficiency

1. ```staging.py``` is most similar to the original code and is hanging around to check that the newer version work properly. This is the slowest.
2. ```staging_multi.py``` adds a pool of worker threads to speed up the downloads
3. ```staging_multi2.py``` builds on ```staging_multi.py``` by eliminating duplicate queries and aggregating inserts into single insert statements. This is the fastest.



## Installation Notes
The original installation instructions found below are pretty good, so please follow those after reading my notes here.

I'm using Windows 8, and I've updated the requirement version for ```psycopg2``` to ```2.6.1```. On my machine, I can ```pip install -r requirements.txt``` without any errors. Hopefully you can to.

I would add that I am using the PowerShell wrapper for virtualenv with good success, and highly recommend [this tutorial](http://www.tylerbutler.com/2012/05/how-to-install-python-pip-and-virtualenv-on-windows-with-powershell/) so you can use it too.





# Original Documentation (Unchanged)

## Overview
A Python project to extract, transform, and load NBA data into a PostgreSQL database.

### Requirements

This project was built with Python 2.7.5, PostgreSQL 9.4.0. That does not mean it won't work in Python 3 or PostgreSQL 9.4, as I haven't tested that yet. It should work on both Windows and Unix operating systems. I think it's best that you create your nbadb within a virtual environment for easy replication.

In your nbadb folder, start a ```virtualenv``` instance (see the [virtualenv docs](http://virtualenv.readthedocs.org/en/latest/virtualenv.html) for more information) and install the required modules:

```
$ virtualenv ENV
$ source ENV/bin/activate # For Unix machines
$ \path\to\ENV\Scripts\activate # For Windows machines
$ pip install -r requirements.txt
```

If you are on a Windows machine and are unable to install ```psycopg2``` with the message 'error: Unable to find vcvarsall.bat,' you will need to install ```psycopg2``` directly as this is a known issue with installing ```psycopg2``` on Windows. To do so, run the following:

```
$ easy_install http://stickpeople.com/projects/python/win-psycopg/2.5.3/psycopg2-2.5.3.win32-py2.6-pg9.3.4-release.exe
```

For more details, see the [following link](http://stackoverflow.com/questions/5382801/where-can-i-download-binary-eggs-with-psycopg2-for-windows/5383266#5383266).

### Configuration

Update your config.ini file with your PostgreSQL credentials. You will need to create a new database called 'nbadb,' which you can do so by using PostgreSQL's ```createdb``` wrapper statement in command line or executing a ```CREATE DATABASE``` statement in a psql interpreter.



## ETL Details (Totally deprecated, left for posterity)

### Staging Layer

nbadb starts by loading raw data from the source *as-is* and dumping them into staging tables. The source includes data from scoreboards, box scores, play-by-play logs, and shot chart detail. There are also data on players, including player profile information, player shot logs, and player rebound logs.

To load data from the entire 2013-14 season into staging tables:
```
$ python load_staging.py 2013-10-29 2014-04-16 # start to end of 2013-14 regular season
$ python update_players.py 2013-14 # loads data for 2013-14 season only
```

To load data from the beginning of the 2014-15 season until yesterday's games:
```
$ python load_staging.py # if no argument, start date set at '2014-10-28'
$ python update_players.py # if no argument, loads data for 2014-15 season only
```

To drop all staging tables (including the player tables), simply run the drop_staging.py script:
```
$ python drop_staging.py
```

### Reporting Layer

TBD

## Credits
- Data courtesy of NBA.com