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
    'BiasingProcess': [('/ldmx/biasing/process', '{BiasingProcess}')],
    'BiasingVolume': [('/ldmx/biasing/volume', '{BiasingVolume}')],
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
    # split physics process from config
    if not 'PhysicsProcess' in conf_dict: 
        logger.error('PhysicsProcess is not defined in the %s. Job aborted.', config)
        sys.exit(1)
    # ensure both random seeds are set
    if 'RandomSeed1' not in conf_dict or 'RandomSeed2' not in conf_dict:
        logger.error('RandomSeed1 and/or RandomSeed2 is not set in %s. Job aborted.', config)
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


def collect_from_json( infile ):
    #function to convert json nested list to flat metadata list 
    config_dict = {}
    with open(infile, "r") as jf :
        mjson = json.load(jf) 

#    mjson = json.loads( infile )
    if not mjson :
        print "trouble"
    else :
        print "opened "+infile
    config_dict['GunPositionX'] = mjson['sequence'][0]['generators'][0]['position'][0]
    config_dict['GunPositionY'] = mjson['sequence'][0]['generators'][0]['position'][1]
    config_dict['GunPositionZ'] = mjson['sequence'][0]['generators'][0]['position'][2]
    config_dict['GunDirectionX'] = mjson['sequence'][0]['generators'][0]['direction'][0]
    config_dict['GunDirectionY'] = mjson['sequence'][0]['generators'][0]['direction'][1]
    config_dict['GunDirectionZ'] = mjson['sequence'][0]['generators'][0]['direction'][2]
    config_dict['BeamSpotSizeX'] = mjson['sequence'][0]['beamSpotSmear'][0]
    config_dict['BeamSpotSizeY'] = mjson['sequence'][0]['beamSpotSmear'][1]
    config_dict['BeamSpotSizeZ'] = mjson['sequence'][0]['beamSpotSmear'][2]
    config_dict['BeamEnergy'] = mjson['sequence'][0]['generators'][0]['energy']
    config_dict['BeamParticle'] = mjson['sequence'][0]['generators'][0]['particle']
    config_dict['RandomSeed1'] = mjson['sequence'][0]['randomSeeds'][0]
    config_dict['RandomSeed2'] = mjson['sequence'][0]['randomSeeds'][1]
    for params in mjson['sequence'][0]['actions'] :
#        print params
        p = params['class_name']
        key=p.replace("ldmx::", "")
#        print key
        for k, val in params.iteritems() :
            if 'threshold' in k :
                keepKey=key+"_"+k
#                print k
                config_dict[keepKey]=val

                
    config_dict['ROOTCompressionSetting'] = mjson['compressionSetting']
                
    config_dict['Geant4BiasParticle'] = mjson['sequence'][0]['biasing_particle']
    config_dict['Geant4BiasProcess'] = mjson['sequence'][0]['biasing_process']
    config_dict['Geant4BiasVolume'] = mjson['sequence'][0]['biasing_volume']
    config_dict['Geant4BiasThreshold'] = mjson['sequence'][0]['biasing_threshold']
    config_dict['Geant4BiasFactor'] = mjson['sequence'][0]['biasing_factor']
    config_dict['APrimeMass'] = mjson['sequence'][0]['APrimeMass']
    config_dict['DarkBremMethod'] = mjson['sequence'][0]['darkbrem_method']
    config_dict['DarkBremMethodXsecFactor'] = mjson['sequence'][0]['darkbrem_globalxsecfactor']


    #ok. over reco stuff, where names can get confusing.

    isRecon = False 
    for seq in mjson['sequence'] :
#        print seq
        if seq['className'] != "ldmx::Simulator" :  #everything except simulation is reconstruction
            isRecon = True 
        if seq['className'] == "ldmx::EcalDigiProducer" :
            config_dict['EcalDigiGain'] = seq['gain']
            config_dict['EcalDigiPedestal'] = seq['pedestal']
            config_dict['EcalDigiNoiseIntercept'] = seq['noiseIntercept']
            config_dict['EcalDigiNoiseSlope'] = seq['noiseSlope']
            config_dict['EcalDigiPadCapacitance'] = seq['padCapacitance']
            config_dict['EcalDigiReadoutThreshold'] = seq['readoutThreshold']
        elif seq['className'] == "ldmx::EcalVetoProcessor" :
            config_dict['EcalLayers'] = seq['num_ecal_layers']
            config_dict['EcalDiscriminatorCut'] = seq['disc_cut']
        elif seq['className'] == "ldmx::HcalVetoProcessor" :
            config_dict['HcalVetoMaxPE'] = seq['pe_threshold']
            config_dict['HcalVetoMaxTime'] = seq['max_time']
            config_dict['HcalVetoMaxDepth'] = seq['max_depth']
            config_dict['HcalVetoBackMinPE'] = seq['back_min_pe']
        elif seq['className'] == "ldmx::HcalDigiProducer" :
            config_dict['HcalMeanNoiseSiPM'] = seq['meanNoise']
            config_dict['HcalMeVPerMIP'] = seq['mev_per_mip']
            config_dict['HcalPEPerMIP'] = seq['pe_per_mip']
            config_dict['HcalAttLength'] = seq['strip_attenuation_length']
            config_dict['HcalPosResolution'] = seq['strip_position_resolution']

        elif seq['className'] == "ldmx::TrigScintDigiProducer" :
            config_dict['TrigScintMeanNoiseSiPM'] = seq['mean_noise']
            config_dict['TrigScintMeVPerMIP'] = seq['mev_per_mip']
            config_dict['TrigScintPEPerMIP'] = seq['pe_per_mip']

        elif seq['className'] == "ldmx::TrackerHitKiller" :
            config_dict['TrackKillerEfficiency'] = seq['hitEfficiency']

        elif seq['className'] == "ldmx::TriggerProcessor" :
            config_dict['TriggerThreshold'] = seq['threshold']
            config_dict['TriggerEcalEndLayer'] = seq['end_layer']
            config_dict['TriggerEcalStartLayer'] = seq['start_layer']

        elif seq['className'] == "ldmx::FindableTrackProcessor" :
            config_dict['FindableTrackWasRun'] = 1
        elif seq['className'] == "ldmx::TrackerVetoProcessor" :
            config_dict['TrackerVetoWasRun'] = 1
            
    config_dict['IsRecon'] = isRecon
                
                
#    print mjson['sequence'][0]['generators'][0]['position'][0]
#    print config_dict
    print(json.dumps(config_dict, indent = 4))
    return config_dict


def collect_meta(conf_dict, mac_dict):
    meta = {
        'IsSimulation': True,
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
    parser.add_argument('action', choices=['generate-mac', 'collect-metadata', 'test'],
                        help='Helper action to perform')
    return parser


if __name__ == '__main__':
    # parse arguments
    cmd_args = get_parser().parse_args()
    loglevel = getattr(logging, cmd_args.debug, 30)
    logger.setLevel(loglevel)

    # config is parsed for any action
#    conf_dict = parse_ldmx_config(cmd_args.config)

    # template substitution (RTE stage 1)
    if cmd_args.action == 'generate-mac':
        mac_dict = parse_mac(cmd_args.template)
        substitute_mac(mac_dict, conf_dict)
        assemble_mac(mac_dict, cmd_args.mac)
        # print values for bash eval
        print_eval(conf_dict)
    elif cmd_args.action == 'test' :
        collect_from_json( "metadata_ldmx_v12_ecal_pn_run0.json" )
    elif cmd_args.action == 'collect-metadata':
        mac_dict = parse_mac(cmd_args.mac)
        meta = collect_meta(conf_dict, mac_dict)
        print(meta['DataLocation'])
        with open(cmd_args.json_metadata, 'w') as meta_f:
            json.dump(meta, meta_f)

