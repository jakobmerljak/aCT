#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import argparse
import sys
import os
import json
import hashlib
import zlib
import time

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
#    if 'RandomSeed1' not in conf_dict or 'RandomSeed2' not in conf_dict:
#        logger.error('RandomSeed1 and/or RandomSeed2 is not set in %s. Job aborted.', config)
#        sys.exit(1)
    # mandatory options
    for opt in ['DetectorVersion', 'FieldMap']:
        if opt not in conf_dict:
            logger.error('%s is not defined in the %s. Job aborted.', opt, config)
            sys.exit(1)
    # ensure FileName is set to something
    if 'FileName' not in conf_dict:
        conf_dict['FileName'] = 'output.root'
    #batch id will be used for storage directory structure. Should always be set.
    if 'BatchID' not in conf_dict:
        logger.error('BatchID is not defined in the %s. Needed for storage directory structure. Job aborted.', config)
        sys.exit(1)

    return conf_dict


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
    try:
        with open(infile, "r") as jf :
            mjson = json.load(jf)
    except Exception as e:
        logger.error('Failed to open {}: {}'.format(infile, str(e)))
        sys.exit(1)

    logger.info('Opened {}'.format(infile))
    if 'generators' in mjson['sequence'][0] :
        config_dict['GunPositionX[mm]']  = mjson['sequence'][0]['generators'][0]['position'][0] if 'position' in mjson['sequence'][0]['generators'][0] else None
        config_dict['GunPositionY[mm]']  = mjson['sequence'][0]['generators'][0]['position'][1] if 'position' in mjson['sequence'][0]['generators'][0] else None
        config_dict['GunPositionZ[mm]']  = mjson['sequence'][0]['generators'][0]['position'][2] if 'position' in mjson['sequence'][0]['generators'][0] else None
        config_dict['MomentumVectorX'] = mjson['sequence'][0]['generators'][0]['direction'][0] if 'direction' in mjson['sequence'][0]['generators'][0] else None
        config_dict['MomentumVectorY'] = mjson['sequence'][0]['generators'][0]['direction'][1] if 'direction' in mjson['sequence'][0]['generators'][0] else None
        config_dict['MomentumVectorZ'] = mjson['sequence'][0]['generators'][0]['direction'][2] if 'direction' in mjson['sequence'][0]['generators'][0] else None
        config_dict['BeamEnergy']    = mjson['sequence'][0]['generators'][0]['energy']  if 'energy' in mjson['sequence'][0]['generators'][0] else None
        config_dict['BeamParticle']  = mjson['sequence'][0]['generators'][0]['particle'] if 'particle' in mjson['sequence'][0]['generators'][0] else None
        #or, if we're using the multiparticle gun, which has different names and conventions for the same parameters
        if not config_dict['GunPositionX[mm]'] :
            config_dict['GunPositionX[mm]']  = mjson['sequence'][0]['generators'][0]['vertex'][0] if 'vertex' in mjson['sequence'][0]['generators'][0] else None
            config_dict['GunPositionY[mm]']  = mjson['sequence'][0]['generators'][0]['vertex'][1] if 'vertex' in mjson['sequence'][0]['generators'][0] else None
            config_dict['GunPositionZ[mm]']  = mjson['sequence'][0]['generators'][0]['vertex'][2] if 'vertex' in mjson['sequence'][0]['generators'][0] else None
        if not config_dict['MomentumVectorX'] :
            config_dict['MomentumVectorX'] = mjson['sequence'][0]['generators'][0]['momentum'][0] if 'momentum' in mjson['sequence'][0]['generators'][0] else None
            config_dict['MomentumVectorY'] = mjson['sequence'][0]['generators'][0]['momentum'][1] if 'momentum' in mjson['sequence'][0]['generators'][0] else None
            config_dict['MomentumVectorZ'] = mjson['sequence'][0]['generators'][0]['momentum'][2] if 'momentum' in mjson['sequence'][0]['generators'][0] else None
        if not config_dict['BeamEnergy'] :
            px = float( str(config_dict['MomentumVectorX']) )
            py = float( str(config_dict['MomentumVectorY']) )
            pz = float( str(config_dict['MomentumVectorZ']) )   #config_dict['MomentumVectorZ'])
            import math
            energy = int( math.sqrt( px*px + py*py + pz*pz ) + 0.5 )
            # now use this to normalise the momentum vector
            config_dict['MomentumVectorX'] = px/energy
            config_dict['MomentumVectorY'] = py/energy
            config_dict['MomentumVectorZ'] = pz/energy
            #and then set the beam energy. first get the units right
            while energy > 999 :
                energy = energy/1000.
                
            config_dict['BeamEnergy'] = energy
        if not config_dict['BeamParticle'] :
            config_dict['BeamParticle']  = mjson['sequence'][0]['generators'][0]['pdgID'] if 'pdgID' in mjson['sequence'][0]['generators'][0] else None
        #note: defaults to 1, rather than "None"
        config_dict['nBeamParticles']  = mjson['sequence'][0]['generators'][0]['nParticles'] if 'nParticles' in mjson['sequence'][0]['generators'][0] else 1
            
        
    if 'beamSpotSmear' in mjson['sequence'][0] :
        config_dict['BeamSpotSizeX[mm]'] = mjson['sequence'][0]['beamSpotSmear'][0]
        config_dict['BeamSpotSizeY[mm]'] = mjson['sequence'][0]['beamSpotSmear'][1]
        config_dict['BeamSpotSizeZ[mm]'] = mjson['sequence'][0]['beamSpotSmear'][2]

    if 'runNumber' in mjson['sequence'][0] :
        config_dict['RunNumber'] = mjson['sequence'][0]['runNumber']
    elif 'run' in mjson :
        config_dict['RunNumber'] = mjson['run']
    else :
        logger.error('RunNumber is not set in %s. Job aborted.', infile)
        sys.exit(1)

    if 'randomSeeds' in mjson['sequence'][0] :
        config_dict['RandomSeed1'] = mjson['sequence'][0]['randomSeeds'][0]
        config_dict['RandomSeed2'] = mjson['sequence'][0]['randomSeeds'][1]
    else :
        logger.error('RandomSeed1 and/or RandomSeed2 is not set in %s. Job aborted.', infile)
        sys.exit(1)

    if 'actions' in mjson['sequence'][0] :
        for params in mjson['sequence'][0]['actions'] :
            p = params['class_name']
            key=p.replace("ldmx::", "")
            for k, val in params.iteritems() :
                if 'threshold' in k :
                    keepKey=key+"_"+k+'[MeV]'
                    config_dict[keepKey]=val

    config_dict['Geant4BiasParticle']  = mjson['sequence'][0]['biasing_particle'] if 'biasing_particle' in  mjson['sequence'][0] else None
    config_dict['Geant4BiasProcess']   = mjson['sequence'][0]['biasing_process'] if 'biasing_process' in  mjson['sequence'][0] else None
    config_dict['Geant4BiasVolume']    = mjson['sequence'][0]['biasing_volume'] if 'biasing_volume' in  mjson['sequence'][0] else None
    config_dict['Geant4BiasThreshold[MeV]'] = mjson['sequence'][0]['biasing_threshold'] if 'biasing_threshold' in  mjson['sequence'][0] else None
    config_dict['Geant4BiasFactor']    = mjson['sequence'][0]['biasing_factor'] if 'biasing_factor' in  mjson['sequence'][0] else None
    config_dict['APrimeMass']          = mjson['sequence'][0]['APrimeMass'] if 'APrimeMass' in  mjson['sequence'][0] else None
    #let these depend on if we are actually generating signal 
    config_dict['DarkBremMethod']      = mjson['sequence'][0]['darkbrem_method'] if  config_dict['APrimeMass']  and 'darkbrem_method' in  mjson['sequence'][0] else None
    config_dict['DarkBremMethodXsecFactor'] = mjson['sequence'][0]['darkbrem_globalxsecfactor'] if config_dict['APrimeMass'] and 'darkbrem_globalxsecfactor' in  mjson['sequence'][0] else None

    #ok. over reco stuff, where parameter names can get confusing.
    # add here as more processors are included
    # not putting in protections here for every possible parameter name, better to let a test job fail if the parameter naming has changed
    isRecon = False 
    isTriggerSkim = False 
    for seq in mjson['sequence'] :
        if seq['className'] != "ldmx::Simulator" :  #everything except simulation is reconstruction
            isRecon = True 
            procName=seq['className']
            procName=procName.replace("ldmx::", "")
            procName=procName.replace("Producer", "")
            procName=procName.replace("Processor", "")
        if seq['className'] == "ldmx::EcalDigiProducer" :
            config_dict[procName+'Gain'] = seq['gain']
            config_dict[procName+'Pedestal'] = seq['pedestal']
            config_dict[procName+'NoiseIntercept'] = seq['noiseIntercept']
            config_dict[procName+'NoiseSlope'] = seq['noiseSlope']
            config_dict[procName+'PadCapacitance'] = seq['padCapacitance']
            config_dict[procName+'ReadoutThreshold'] = seq['readoutThreshold']
        elif seq['className'] == "ldmx::EcalRecProducer" :
            config_dict[procName+'SecondOrderEnergyCorrection'] = seq['secondOrderEnergyCorrection']
        elif seq['className'] == "ldmx::EcalVetoProcessor" :
            config_dict[procName+'Layers'] = seq['num_ecal_layers']
            config_dict[procName+'DiscriminatorCut'] = seq['disc_cut']
        elif seq['className'] == "ldmx::HcalVetoProcessor" :
            config_dict[procName+'MaxPE'] = seq['pe_threshold']
            config_dict[procName+'MaxTime[ns]'] = seq['max_time']
            config_dict[procName+'MaxDepth[cm]'] = seq['max_depth']
            config_dict[procName+'BackMinPE'] = seq['back_min_pe']
        elif seq['className'] == "ldmx::HcalDigiProducer" :
            config_dict[procName+'MeanNoiseSiPM'] = seq['meanNoise']
            config_dict[procName+'MeVPerMIP'] = seq['mev_per_mip']
            config_dict[procName+'PEPerMIP'] = seq['pe_per_mip']
            config_dict[procName+'AttLength[m]'] = seq['strip_attenuation_length']
            config_dict[procName+'PosResolution[mm]'] = seq['strip_position_resolution']
        elif seq['className'] == "ldmx::TrigScintDigiProducer" :
            config_dict[procName+'MeanNoiseSiPM'] = seq['mean_noise']
            config_dict[procName+'MeVPerMIP'] = seq['mev_per_mip']
            config_dict[procName+'PEPerMIP'] = seq['pe_per_mip']
        elif seq['className'] == "ldmx::TrackerHitKiller" :
            config_dict[procName+'Efficiency'] = seq['hitEfficiency']
        elif seq['className'] == "ldmx::TriggerProcessor" :
            config_dict[procName+'Threshold[MeV]'] = seq['threshold']
            config_dict[procName+'EcalEndLayer'] = seq['end_layer']
            config_dict[procName+'EcalStartLayer'] = seq['start_layer']
        elif seq['className'] == "ldmx::FindableTrackProcessor" :
            config_dict[procName+'WasRun'] = 1
        elif seq['className'] == "ldmx::TrackerVetoProcessor" :
            config_dict[procName+'WasRun'] = 1

    config_dict['IsRecon'] = isRecon
    config_dict['IsTriggerSkim'] = isTriggerSkim

    config_dict['ROOTCompressionSetting'] = mjson['compressionSetting'] if 'compressionSetting' in mjson else None 

    config_dict['NumberOfEvents'] = mjson['maxEvents'] if 'maxEvents' in mjson else None 

    logger.info(json.dumps(config_dict, indent = 2))
    return config_dict


def job_starttime(starttime_f='.ldmx.job.starttime'):
    if os.path.exists(starttime_f):
        with open(starttime_f, 'r') as fd:
            return int(fd.read())
    else:
        current_time = int(time.time())
        with open(starttime_f, 'w') as fd:
            fd.write('{0}'.format(current_time))
            return current_time

def set_remote_output(conf_dict, meta):
    # Check for remote location and construct URL
    # GRID_GLOBAL_JOBHOST is available from ARC 6.8
    cehost = os.environ.get('GRID_GLOBAL_JOBHOST')
    if 'FinalOutputDestination' in conf_dict and 'FinalOutputBasePath' in conf_dict \
      and cehost not in conf_dict.get('NoUploadSites', '').split(','):
        pfn = conf_dict['FinalOutputBasePath']
        while pfn.endswith('/'):
            pfn = pfn[:-1]
        pfn += '/{Scope}/v{DetectorVersion}/{BeamEnergy}GeV/{BatchID}/mc_{SampleId}_run{RunNumber}_t{FileCreationTime}.root'.format(**meta)
        meta['remote_output'] = {'rse': conf_dict['FinalOutputDestination'],
                                 'pfn': pfn}
        # Add to ARC output list
        with open('output.files', 'w') as f:
            f.write('{} {}'.format(conf_dict['FileName'], pfn))
    else:
        # Create empty output files list
        with open('output.files', 'w') as f:
            pass

def collect_meta(conf_dict, json_file):
    meta = collect_from_json(json_file)

    # conf
    meta['IsSimulation'] = True
    for fromconf in ['Scope', 'SampleId', 'BatchID', 'PhysicsProcess', 'DetectorVersion']:
        meta[fromconf] = conf_dict[fromconf] if fromconf in conf_dict else None
    meta['ElectronNumber'] = int(conf_dict['ElectronNumber']) if 'ElectronNumber' in conf_dict else None
    meta['MagneticFieldmap'] = conf_dict['FieldMap'] if 'FieldMap' in conf_dict else None
    # env
    if 'ACCOUNTING_WN_INSTANCE' in os.environ:
        meta['LdmxImage'] = os.environ['ACCOUNTING_WN_INSTANCE']
    elif 'SINGULARITY_IMAGE' in os.environ:
        meta['LdmxImage'] = os.environ['SINGULARITY_IMAGE'].split('/')[-1]
    else:
        meta['LdmxImage'] = None
    meta['ARCCEJobID'] = os.environ['GRID_GLOBAL_JOBID'].split('/')[-1] if 'GRID_GLOBAL_JOBID' in os.environ else None
    meta['FileCreationTime'] = int(time.time())
    meta['Walltime'] = meta['FileCreationTime'] - job_starttime()

    # Check output file actually exists
    if not os.path.exists(conf_dict.get('FileName', '')):
        logger.error('Output file {} does not exist!'.format(conf_dict.get('FileName', '')))
        return meta

    data_location = os.environ['LDMX_STORAGE_BASE']
    data_location += '/ldmx/mc-data/{Scope}/v{DetectorVersion}/{BeamEnergy}GeV/{BatchID}/mc_{SampleId}_run{RunNumber}_t{FileCreationTime}.root'.format(**meta)
    meta['DataLocation'] = data_location

    # Rucio metadata
    meta['scope'] = meta['Scope']
    meta['name'] = os.path.basename(data_location)
    meta['datasetscope'] = meta['Scope']
    meta['datasetname'] = meta['BatchID']
    meta['containerscope'] = meta['Scope']
    meta['containername'] = meta['SampleId']

    meta['bytes'] = os.stat(conf_dict['FileName']).st_size
    (meta['md5'], meta['adler32']) = calculate_md5_adler32_checksum(conf_dict['FileName'])

    set_remote_output(conf_dict, meta)
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
    parser.add_argument('-m', '--metaDump', action='store', default='parameterDump.json',
                        help='LDMX Production simulation parameter dump JSON file')
    parser.add_argument('-j', '--json-metadata', action='store', default='rucio.metadata',
                        help='LDMX Production simulation JSON metadata file')
    parser.add_argument('action', choices=['init', 'collect-metadata', 'test'],
                        help='Helper action to perform')
    return parser


if __name__ == '__main__':
    # parse arguments
    cmd_args = get_parser().parse_args()
    loglevel = getattr(logging, cmd_args.debug, 30)
    logger.setLevel(loglevel)

    # config is parsed for any action
    conf_dict = parse_ldmx_config(cmd_args.config)

    # metadata extraction from job parameter dump
    if cmd_args.action == 'test' :
        collect_from_json( "parameterDump.json" )
    elif cmd_args.action == 'init':
        # store job start time
        job_starttime()
        # print values for bash eval
        print_eval(conf_dict)
    elif cmd_args.action == 'collect-metadata':
        meta = collect_meta(conf_dict, cmd_args.metaDump)
        if 'DataLocation' not in meta:
            sys.exit(1)
        print('export FINALOUTPUTFILE="{DataLocation}"'.format(**meta))
        with open(cmd_args.json_metadata, 'w') as meta_f:
            json.dump(meta, meta_f)


