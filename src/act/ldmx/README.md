# LDMX Module

This module contains agents for handling jobs for the LDMX experiment

# Configuration

Example app configuration:

```
<config>

<modules>
    <app>act.ldmx</app>
</modules>

<jobs>
    <bufferdir>/path/to/bufferdir</bufferdir>
    <maxsubmitted>2</maxsubmitted>
</jobs>

<executable>
    <wrapper>/path/to/LDCS-repo/LDCS/helperScripts/ldmxsim.sh</wrapper>
    <ruciohelper>/path/to/LDCS-repo/LDCS/helperScripts/ldmx-simprod-rte-helper.py</ruciohelper>
    <simprodrte>LDMX-SIMPROD-3.0</simprodrte>
</executable>

</config>
```

- `bufferdir` is a directory for storing temporary configuration files generated per job by aCT
- `wrapper` is the wrapper script which runs the job on grid worker nodes
- `ruciohelper` is another script which runs at the end of the job to extract rucio metadata of output files
- `simprodrte` is the RTE required to run the simulation. The LDMX software RTE is specified in the job configuration.

The wrapper and rucio helper scripts are maintained in the LDCS repo at https://github.com/LDMX-Software/LDCS/tree/master/helperScripts

For job submission and management see the "ACT operations" doc in the LDCS google drive folder.
