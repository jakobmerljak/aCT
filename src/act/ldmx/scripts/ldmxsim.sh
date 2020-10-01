#!/bin/bash
#
# Wrapper script for LDMX simulation
#

echo -e "ldmxsim.sh running on host $(/bin/hostname -f)\n"

# Check all files are present
for f in "ldmxproduction.config" "ldmxjob.py" "ldmx-simprod-rte-helper.py"; do
  if [ ! -f "$f" ]; then
    echo "ERROR: LDMX Simulation production job requires $f file but it is missing" >&2
    exit 1
  fi
done

echo -e "ldmxproduction.config:\n"
cat ldmxproduction.config
echo
echo

# Check env vars are defined properly by RTE
if [ -z "$LDMX_STORAGE_BASE" ]; then
  echo "ERROR: ARC CE admin should define LDMX_STORAGE_BASE with arcctl rte params-set"
  exit 1
fi

if [ -z "$SINGULARITY_IMAGE" ]; then
  echo "ERROR: ARC CE admin should define SINGULARITY_IMAGE with arcctl rte params-set"
  exit 1
fi

# Check singularity is installed
type singularity
if [ $? -ne 0 ]; then
  echo "ERROR: Singularity installation on the worker nodes is required to run LDMX software"
  exit 1
fi

# Initialise some parameters
eval $( python ldmx-simprod-rte-helper.py -c ldmxproduction.config init )
echo -e "Output data file is $OUTPUTDATAFILE\n"

# Start the simulation container
echo -e "Starting Singularity image $SINGULARITY_IMAGE\n"
singularity run $SINGULARITY_OPTIONS --home "$PWD" "$SINGULARITY_IMAGE" . ldmxjob.py
RET=$?

if [ $RET -ne 0 ]; then
  echo "Singularity exited with code $RET"
  exit $RET
fi

echo -e "\nSingularity exited normally, proceeding with post-processing...\n"

# Post processing to extract metadata for rucio
eval $( python ldmx-simprod-rte-helper.py -j rucio.metadata -c ldmxproduction.config collect-metadata )
if [ -z "$FINALOUTPUTFILE" ]; then
  echo "Post-processing script failed!"
  exit 1
fi

echo "Copying $OUTPUTDATAFILE to $FINALOUTPUTFILE"
mkdir -p "${FINALOUTPUTFILE%/*}"
cp "$OUTPUTDATAFILE" "$FINALOUTPUTFILE"
if [ $? -ne 0 ]; then
  echo "Failed to copy output to final destination"
  exit 1
fi

# Success
echo "Success, exiting..."
exit 0

