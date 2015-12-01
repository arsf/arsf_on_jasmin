ARSF on JASMIN
================

Scripts for running ARSF code on the [JASMIN/CEMS](http://jasmin.ac.uk/) platform.
Note for general enquires about JASMIN check the [FAQ](http://jasmin.ac.uk/faq/).

Setup
------

The first thing to to is apply for access to JASMIN/CEMS (http://jasmin.ac.uk/workflow/)
As part of the setup process you will be given the option to apply for access to
a 'Group Workspace'. If you don't have a specific workspace for your project you
can apply for access to the 'CEMS - ARSF Processing' workspace.

If you want to process data archived at [NEODC](http://neodc.nerc.ac.uk/)
register for access to the ARSF archive (if you haven't done so already). The
ARSF archive (up to 2012) is available on the JASMIN platform under `/neodc/arsf/`

```bash
module avail
```

To load [APL](https://github.com/arsf/apl), [LAStools](https://github.com/LAStools/LAStools/)
and the [ARSF DEM Scripts](https://github.com/pmlrsg/arsf_dem_scripts) use the
following command:

```bash
module load contrib/arsf/apl/3.5.6 contrib/arsf/lastools/20150925 contrib/arsf/arsf_dem_scripts/0.1.2
```

Processing hyperspectral data using APL
-----------------------------------------

You can use the APL commands from one of the analysis nodes for processing data
However, to get the best performance is is recommended to use the
[LOTUS](http://jasmin.ac.uk/how-to-use-jasmin/lotus-documentation/) system
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
