NERC-ARF on JASMIN
=====================

Scripts for running NERC-ARF code on the [JASMIN](http://jasmin.ac.uk/) platform.

Setup
------

The first thing to to is [apply for a JASMIN account](https://help.jasmin.ac.uk/article/4435-get-a-jasmin-account)
As part of the setup process you will be given the option to apply for access to
a 'Group Workspace'. If you don't have a specific workspace for your project you
can apply for access to the 'CEMS - ARSF Processing' workspace.

If you want to process data archived at [CEDA](http://www.ceda.ac.uk/)
[register for access to the ARSF archive](https://services.ceda.ac.uk/cedasite/resreg/application?attributeid=arsf) (if you haven't done so already).
The NERC-ARF archive is available on the JASMIN platform under `/neodc/arsf/`

Once you have access to JASMIN and have logged into one of the analysis VMs
(e.g., `cems-sci1.cems.rl.ac.uk`) you need to load the ARSF programs.
These aren't available by default but can be loaded using the modules
system.
To list all available modules use:

```bash
module avail
```
The NERC-ARF modules are listed under `contrib/arsf`.

To load [APL](https://github.com/arsf/apl), [LAStools](https://github.com/LAStools/LAStools/)
and the [NERC-ARF DEM Scripts](https://github.com/pmlrsg/arsf_dem_scripts) use the
following command:

```bash
module load contrib/arsf/apl contrib/arsf/lastools contrib/arsf/arsf_dem_scripts
```
Note you will need to do this every time you log on (or you can add to your
`.bashrc`).

To make it easier to find data by year and day you can use the `neodc_pdir` script.
For example to go to the directory containing data for 2015/249 use:

```
source neodc_pdir 2015 249
```

Processing hyperspectral data using APL
-----------------------------------------

You can use the APL commands from one of the analysis nodes for processing data.
However, to get the best performance is is recommended to use the
[LOTUS](https://help.jasmin.ac.uk/article/110-lotus-overview) system
and submit each line as a separate job. A script has been created to simplify
creating and submitting jobs to process each line. Typical usage is:

```bash
submit_apl_lotus.py --inlevel1b flightlines/level1b/ \
                    --outmapped flightlines/mapped/ \
                    --outproj osng \
                    --dem dem/GB14_00-2014_216_Little_Riss_Fenix.dem \
                    --outscripts . \
                    --zip
```

After checking the .bsub files look OK submit by adding the flag `--submit`
to the command above. You can check the status of jobs using the `bsub` command.

Processing LiDAR Data
-----------------------

To convert between LAS and ASCII formats and compress files using [LASzip](http://www.laszip.org/) the open source utilities from [LAStools](http://rapidlasso.com/lastools/) are available. For more details on using with ARSF LiDAR data see the ARSF-DAN wiki: https://nerc-arf-dan.pml.ac.uk/trac/wiki/FAQ/las2ascii

To create a DTM/DSM from LAS files the [NERC-ARF DEM Scripts](http://github.com/pmlrsg/arsf_dem_scripts) can be used.
The script 'submit_las_to_dsm_lotus.py' provides an example of creating a DSM for multiple lines in parallel using the LOTUS sustem.

Usage is similar to 'submit_apl_lotus.py':

```bash
submit_las_to_dsm_lotus.py --inlas flightlines/discrete_laser/las1.2 \
                           --outdir dsm
                           --projection UKBNG \
                           --outscripts . \
                           --resolution 2 \
                           --submit
```

Aerial Photography Data
------------------------

Some of the scanned aerial photography is stored in a compressed JP2 format, it is often easier to work with as a TIFF. To convert files the `gdal_translate` command can be used.
The following script will submit each image as a separate LOTUS grid job.

```bash
convert_jp2_aerial_photos_tiff_lotus.py -i input_jp2 -o output_dir
```
