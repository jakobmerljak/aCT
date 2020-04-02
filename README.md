# ARC Control Tower

ARC Control Tower (aCT) is a system for submitting and managing payloads on ARC (and other) Computing Elements. It is used as an interface between Panda and Grid sites for the ATLAS experiment at CERN.

[![Build Status](https://travis-ci.com/ARCControlTower/aCT.svg?branch=master)](https://travis-ci.com/ARCControlTower/aCT)

(note that Travis build will fail until python3 ARC packages are available in Ubuntu)

# Installing

aCT requires python >= 3.6 and is designed to run in a python virtual environment.

## Mandatory dependencies

For CentOS 7:

`# yum install epel-release`

`# yum install python-pip nordugrid-arc6-client python36-nordugrid-arc6 git`

ARC python bindings are not available in pip so must be installed as a system package.

## Optional dependencies

`# yum install nordugrid-arc6-plugins-gridftpjob` - for submission to ARC's GridFTP interface

`# yum install nordugrid-arc6-plugins-xrootd` - for aCT to validate any output files written using the xrootd protocol

`# yum install condor` - if aCT will submit to HTCondor-CE or CREAM CE

## Setting up the virtualenv

```
$ python3 -m venv aCT
$ source aCT/bin/activate
$ pip install git+https://github.com/ARCControlTower/aCT
```

Then one of two workarounds must be done to use ARC modules in the virtualenv, either create symlinks inside the virtualenv, eg
```
$ ln -s /usr/lib64/python3.6/site-packages/_arc.cpython-36m-x86_64-linux-gnu.so aCT/lib64/python3.6/site-packages/
$ ln -s /usr/lib64/python3.6/site-packages/arc aCT/lib64/python3.6/site-packages/arc
```
or add the system packages to your python path
```
export PYTHONPATH=/usr/lib64/python3.6/site-packages/arc
```
The actual paths may depend on your system and python version.

aCT requires a database. MySQL/MariaDB is the only officially supported database but work is ongoing to use sqlite. __Note__ that MySQL/MariaDB >= 5.6 is required whereas the default on CentOS 7 is 5.5.

# Configuring

aCT is configured with 2 configuration files, `aCTConfigARC.xml` and optional `aCTConfigAPP.xml`. These files are searched for in the following places in order until found:
```
$ACTCONFIGARC
$VIRTUAL_ENV/etc/act/aCTConfigARC.xml
/etc/act/aCTConfigARC.xml
./aCTConfigARC.xml
```
and the same for aCTConfigAPP.xml. Configuration templates can be found in etc/act in the virtualenv.

Once configuration is set up, the `actbootstrap` tool should be used to create the necessary database tables.

# Running

The `actmain` tool starts and stops aCT
```
$ actmain start
Starting aCT... 
$ actmain stop
Stopping aCT...  stopped
```

# Administration

Several tools exist to help administer aCT

- `actreport`: shows a summary of job states and sites for all the jobs in the database
- `actbootstrap`: create database tables
- `actheartbeatwatchdog`: checks the database for jobs that have not sent heartbeats for a given time and manually send the heartbeat
- `actcriticalmonitor`: checks logs for critical error messages in the last hour - can be run in a cron to send emails

# Client tools

__Experimental__ client tools exist which allow job management through simple command line tools (`actsub`, `actstat`, etc). These tools allow aCT to be used as a generic job submission engine, independent from the ATLAS part.
