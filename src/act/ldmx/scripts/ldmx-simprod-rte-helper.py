#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import argparse
import sys
import os
import json
import hashlib
import zlib

config_to_mac_substs = {
    'RandomSeed1': [('/random/setSeeds', '{RandomSeed1} {RandomSeed2}')],
    'NumberofEvents': [('/run/beamOn', '{NumberofEvents}')],
    'BeamEnergy': [('/gun/energy', '{BeamEnergy} GeV')],
    'FileName': [('/ldmx/persistency/root/file', '{FileName}')],
    'Geant4BiasThreshold': [
        ('/ldmx/biasing/threshold', '{Geant4BiasThreshold}'),
        ('/ldmx/biasing/xsec/threshold', '{Geant4BiasThreshold}')
    ],
    'Geant4BiasFactor': [('/ldmx/biasing/xsec/factor', '{Geant4BiasFactor}')],
    'TargetBremFilterRecoil': [('/ldmx/plugins/TargetBremFilter/recoil_threshold', '{TargetBremFilterRecoil}')],
    'TargetBremFilterBrem': [('/ldmx/plugins/TargetBremFilter/brem_threshold', '{TargetBremFilterBrem}')],
    'BeamspotSizeX': [('/ldmx/generators/beamspot/sizeX', '{BeamspotSizeX}')],
    'BeamspotSizeY': [('/ldmx/generators/beamspot/sizeY', '{BeamspotSizeY}')]
}

# logging
logger = logging.getLogger('LDMX.SimProd.Helper')
logger.setLevel(logging.INFO)
log_handler_stderr = logging.StreamHandler()
log_handler_stderr.setFormatter(
    logging.Formatter('[%(asctime)s] [%(name)s] [%(levelname)s] [%(process)d] [%(message)s]'))
logger.addHandler(log_handler_stderr)

# read ldmx.config to dict
def parse_ldmx_config(config='ldmxjob.config'):
    conf_dict = {}
    with open(config, 'r') as conf_f:
        for line in conf_f:
            kv = line.split('=', 2)
            if len(kv) != 2:
                logger.error('Malformed %s line: %s', config, line)
                continue
            conf_dict[kv[0]] = kv[1].strip()
    # ensure both random seeds are set
    if 'RandomSeed1' in conf_dict and 'RandomSeed2' not in conf_dict:
        logger.error('RandomSeed1 is set without RandomSeed2 in %s. Job aborted.', config)
        sys.exit(1)
    # mandatory options
    for opt in ['DetectorVersion', 'FieldMap']:
        if opt not in conf_dict:
            logger.error('%s is not defined in the %s. Job aborted.', opt, config)
            sys.exit(1)
    # ensure FileName is set to something
    if 'FileName' not in conf_dict:
        conf_dict['FileName'] = 'output.root'
    return conf_dict


def parse_mac(macfile='ldmxsim.mac.template'):
    mac_dict = {
        'order': [],
        'createtime': int(os.path.getmtime(macfile))
    }
    with open(macfile, 'r') as conf_f:
        for line in conf_f:
            kv = line.split(' ', 1)
            mackey = kv[0].strip()
            mac_dict['order'].append(mackey)
            if len(kv) == 2:
                if mackey not in mac_dict:
                    mac_dict[mackey] = []
                mac_dict[mackey].append(kv[1].strip())
    return mac_dict

def substitute_mac(mac_dict, conf_dict):
    for confkey in config_to_mac_substs.keys():
        if confkey in conf_dict:
            for (mackey, macvalue) in config_to_mac_substs[confkey]:
                if mackey in mac_dict:
                    mac_dict[mackey] = [macvalue.format(**conf_dict)]

def assemble_mac(mac_dict, macfile='ldmxsim.mac'):
    with open(macfile, 'w') as mac_f:
        for mackey in mac_dict['order']:
            mac_f.write(mackey)
            if mackey in mac_dict:
                mac_f.write(' ')
                mac_f.write(mac_dict[mackey].pop(0))
            mac_f.write('\n')

def print_eval(conf_dict):
    print('export DETECTOR="ldmx-det-full-v{DetectorVersion}-fieldmap-magnet"\n'
          'export FIELDMAP="{FieldMap}"\n'
          'export OUTPUTDATAFILE="{FileName}"'.format(**conf_dict))


def calculate_md5_adler32_checksum(file, chunk_size=524288):
    md5 = hashlib.md5()
    adler32 = 1
    with open(file, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            md5.update(chunk)
            adler32 = zlib.adler32(chunk, adler32) & 0xffffffff
    return (md5.hexdigest(), '{:08x}'.format(adler32))


def collect_meta(conf_dict, mac_dict):
    meta = {
        'IsSimulation': True,
        'IsRecon': False
    }
    # conf
    for fromconf in ['Scope', 'SampleId', 'PhysicsProcess', 'DetectorVersion']:
        meta[fromconf] = conf_dict[fromconf] if fromconf in conf_dict else None
    meta['ElectronNumber'] = int(conf_dict['ElectronNumber']) if 'ElectronNumber' in conf_dict else None
    meta['MagneticFieldmap'] = conf_dict['FieldMap'] if 'FieldMap' in conf_dict else None
    # env
    meta['LdmxImage'] = os.environ['ACCOUNTING_WN_INSTANCE'] if 'ACCOUNTING_WN_INSTANCE' in os.environ else None
    meta['ARCCEJobID'] = os.environ['GRID_GLOBAL_JOBID'].split('/')[-1] if 'GRID_GLOBAL_JOBID' in os.environ else None
    meta['FileCreationTime'] = int(os.path.getmtime(conf_dict['FileName']))
    meta['Walltime'] = meta['FileCreationTime'] - mac_dict['createtime']
    # mac (strings)
    for mackey, metakey in [
        ('/ldmx/biasing/xsec/particle', 'Geant4BiasParticle'),
        ('/gun/particle', 'BeamParticle'),
        ('/ldmx/plugins/EcalProcessFilter/volume', 'ECalProcessFilterVolume'),
        ('/ldmx/plugins/TargetBremFilter/volume', 'TargetBremFilterVolume'),
        ('/ldmx/plugins/TrackFilterPlugin/create', 'TrackfilterType')
    ]:
        if mackey in mac_dict:
            meta[metakey] = mac_dict[mackey][0]
    # mac (numbers)
    for mackey, metakey in [
        ('/run/beamOn', 'NumberofEvents'),
        ('/ldmx/biasing/xsec/threshold', 'Geant4BiasThreshold'),
        ('/ldmx/biasing/xsec/factor', 'Geant4BiasFactor'),
        ('/ldmx/plugins/TargetBremFilter/recoil_threshold', 'TargetBremFilterRecoil'),
        ('/ldmx/plugins/TargetBremFilter/brem_threshold', 'TargetBremFilterBrem'),
        ('/ldmx/generators/beamspot/sizeX', 'BeamspotSizeX'),
        ('/ldmx/generators/beamspot/sizeY', 'BeamspotSizeY'),
    ]:
        if mackey in mac_dict:
            meta[metakey] = float(mac_dict[mackey][0])
    # mac (special handling: arrays, etc)
    if '/ldmx/persistency/root/dropCol' in mac_dict:
        meta['DroppedCollections'] = mac_dict['/ldmx/persistency/root/dropCol']
    else:
        meta['DroppedCollections'] = []

    if '/gun/energy' in mac_dict:
        meta['BeamEnergy'] = float(mac_dict['/gun/energy'][0].split()[0])
    else:
        meta['BeamEnergy'] = None

    if '/gun/position' in mac_dict:
        meta['GunPositionX'] = float(mac_dict['/gun/position'][0].split()[0])
        meta['GunPositionY'] = float(mac_dict['/gun/position'][0].split()[1])
        meta['GunPositionZ'] = float(mac_dict['/gun/position'][0].split()[2])
    else:
        meta['GunPositionX'] = None
        meta['GunPositionY'] = None
        meta['GunPositionZ'] = None

    if '/gun/direction' in mac_dict:
        meta['MomentumVectorX'] = float(mac_dict['/gun/direction'][0].split()[0])
        meta['MomentumVectorY'] = float(mac_dict['/gun/direction'][0].split()[1])
        meta['MomentumVectorZ'] = float(mac_dict['/gun/direction'][0].split()[2])
    else:
        meta['MomentumVectorX'] = None
        meta['MomentumVectorY'] = None
        meta['MomentumVectorZ'] = None

    if '/ldmx/plugins/TrackFilterPlugin/process' in mac_dict:
        meta['TrackfilterProcess'] = mac_dict['/ldmx/plugins/TrackFilterPlugin/process'][0].split()[0]
    else:
        meta['TrackfilterProcess'] = None

    if '/ldmx/plugins/TrackFilterPlugin/region' in mac_dict:
        meta['TrackfilterRegion'] = mac_dict['/ldmx/plugins/TrackFilterPlugin/region'][0].split()[0]
    else:
        meta['TrackfilterRegion'] = None

    if '/random/setSeeds' in mac_dict:
        meta['RandomSeed1'] = int(mac_dict['/random/setSeeds'][0].split()[0])
        meta['RandomSeed2'] = int(mac_dict['/random/setSeeds'][0].split()[1])
    else:
        meta['RandomSeed1'] = None
        meta['RandomSeed2'] = None

    meta['SavingEnabled'] = True
    if '/random/setSavingFlag' in mac_dict and mac_dict['/random/setSavingFlag'][0] == '0':
        meta['SavingEnabled'] = False

    data_location = os.environ['LDMX_STORAGE_BASE']
    data_location += '/ldmx/mc-data/v{DetectorVersion}/{BeamEnergy}GeV/mc_{SampleId}_t{FileCreationTime}.root'.format(**meta)
    meta['DataLocation'] = data_location

    # Rucio metadata
    meta['scope'] = meta['Scope']
    meta['name'] = os.path.basename(data_location)
    meta['datasetscope'] = meta['Scope']
    meta['datasetname'] = meta['SampleId']

    meta['bytes'] = os.stat(conf_dict['FileName']).st_size
    (meta['md5'], meta['adler32']) = calculate_md5_adler32_checksum(conf_dict['FileName'])

    return meta

def get_parser():
    parser = argparse.ArgumentParser(description='LDMX Production Simulation Helper')
    parser.add_argument('-d', '--debug', action='store', default='INFO',
                        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'],
                        help='verbosity level (default is %(default)s)')
    parser.add_argument('-c', '--config', action='store', default='ldmxjob.config',
                        help='LDMX Production simulation job config file')
    parser.add_argument('-t', '--template', action='store', default='ldmxsim.mac.template',
                        help='LDMX Production simulation macro-definition file template')
    parser.add_argument('-m', '--mac', action='store', default='ldmxsim.mac',
                        help='LDMX Production simulation macro-definition file')
    parser.add_argument('-j', '--json-metadata', action='store', default='rucio.metadata',
                        help='LDMX Production simulation JSON metadata file')
    parser.add_argument('action', choices=['generate-mac', 'collect-metadata'],
                        help='Helper action to perform')
    return parser


if __name__ == '__main__':
    # parse arguments
    cmd_args = get_parser().parse_args()
    loglevel = getattr(logging, cmd_args.debug, 30)
    logger.setLevel(loglevel)

    # config is parsed for any action
    conf_dict = parse_ldmx_config(cmd_args.config)

    # config processing substitution (RTE stage 1)
    if cmd_args.action == 'generate-mac':
        if os.path.exists(cmd_args.mac):
            # if mac file is already present in session directory -
            # parse it and get the correct output FileName
            mac_dict = parse_mac(cmd_args.mac)
            if '/ldmx/persistency/root/file' in mac_dict:
                conf_dict['FileName'] = mac_dict['/ldmx/persistency/root/file']
        else:
            # parse template, do substitutions and create mac
            mac_dict = parse_mac(cmd_args.template)
            substitute_mac(mac_dict, conf_dict)
            assemble_mac(mac_dict, cmd_args.mac)
        # print values for bash eval
        print_eval(conf_dict)
    elif cmd_args.action == 'collect-metadata':
        mac_dict = parse_mac(cmd_args.mac)
        meta = collect_meta(conf_dict, mac_dict)
        print(meta['DataLocation'])
        with open(cmd_args.json_metadata, 'w') as meta_f:
            json.dump(meta, meta_f)

