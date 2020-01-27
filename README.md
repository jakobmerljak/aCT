# ARC Control Tower

ARC Control Tower (aCT) is a system for submitting and managing payloads on ARC (and other) Computing Elements. It is used as an interface between Panda and Grid sites for the ATLAS experiment at CERN.

[![Build Status](https://travis-ci.com/ARCControlTower/aCT.svg?branch=master)](https://travis-ci.com/ARCControlTower/aCT)

# Installing

aCT requires python 2.7 and is designed to run in a python virtual environment.

## Mandatory dependencies

For CentOS 7:

`# yum install epel-release`

`# yum install python-pip nordugrid-arc-client python2-nordugrid-arc git`

`# pip install virtualenv`

ARC python bindings are not available in pip so must be installed as a system package

## Optional dependencies

`# yum install nordugrid-arc-plugins-globus` - for submission to ARC's GridFTP interface

`# yum install nordugrid-arc-plugins-xrootd` - for aCT to validate any output files written using the xrootd protocol

`# yum install condor` - if aCT will submit to HTCondor-CE or CREAM CE

## Setting up the virtualenv

```
$ virtualenv aCT
$ source aCT/bin/activate
$ pip install git+https://github.com/ARCControlTower/aCT
```

Then one of two workarounds must be done to use ARC modules in the virtualenv, either create symlinks inside the virtualenv, eg
```
$ ln -s /usr/lib64/python2.7/site-packages/_arc.so aCT/lib64/python2.7/site-packages/_arc.so
$ ln -s /usr/lib64/python2.7/site-packages/arc aCT/lib64/python2.7/site-packages/arc
```
or add the system packages to your python path
```
export PYTHONPATH=/usr/lib64/python2.7/site-packages/arc
```
aCT requires a database. MySQL/MariaDB is the only officially supported database but work is ongoing to use sqlite.

# Configuring

aCT is configured with 2 configuration files, `aCTConfigARC.xml` and `aCTConfigATLAS.xml`. These files are searched for in the following places in order until found:
```
$ACTCONFIGARC
$VIRTUAL_ENV/etc/act/aCTConfigARC.xml
/etc/act/aCTConfigARC.xml
./aCTConfigARC.xml
```
and the same for aCTConfigATLAS.xml. Configuration templates can be found in etc/act in the virtualenv.

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
