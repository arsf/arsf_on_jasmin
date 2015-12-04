#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
"""
A script to create DSMs from LAS files using:

https://github.com/pmlrsg/arsf_dem_scripts

Submits each LAS file as a separate job.

Author: Dan Clewley
Creation Date: 04/12/2015

"""
from __future__ import print_function
import argparse
import glob
import os
import subprocess

DEFAULT_PIXEL_SIZE = 2

def write_bsub_script_for_dict(flight_parameters, output_filename):
   """
   Write dictionary of flight parameters to a text file.
   """
   bsub_script_text = '''#!/bin/bash
#BSUB -J {basename}
#BSUB –o {scripts_dir}/%J.o
#BSUB –e {scripts_dir}/%J.e
#BSUB –q lotus
#BSUB -W 01:00
#BSUB -n 1

las_to_dsm.py --projection {projection} --resolution {resolution} -o {out_dsm} {input_las}
'''.format(**flight_parameters)

   with open(output_filename,'w') as f:
      f.write(bsub_script_text)

if __name__ == '__main__':
   parser = argparse.ArgumentParser(
   description='''Produce scripts for submitting data to be processed on as LOTUS jobs using bsub''',)

   parser.add_argument('--inlas', type=str,
                        help='Input LAS directory',required=True)
   parser.add_argument('--outdir', type=str,
                        default=None,
                        help='Output DSM directory',required=True)
   parser.add_argument('--projection', type=str,
                        help='Output projection. E.g., UKBNG, UTM30N',
                        required=True, default=None)
   parser.add_argument('--outscripts', type=str,
                        help='Output directory for bsub scripts (default = current directory)',
                        default='.',
                        required=False)
   parser.add_argument('--resolution', type=float,
                        help='Pixel size for mapped files',
                        required=False, default=DEFAULT_PIXEL_SIZE)
   parser.add_argument('--submit', action='store_true',
                        help='Submit jobs for processing.',
                        required=False, default=False)
   args = parser.parse_args()

   # Convert to absolute paths
   output_dir = os.path.abspath(args.outdir)
   output_scripts = os.path.abspath(args.outscripts)

   # Create output directories if they don't exist.
   if not os.path.isdir(output_dir):
      print('Output directory "{}" does not exist - creating it now'.format(output_dir))
      os.makedirs(output_dir)

   if not os.path.isdir(output_scripts):
      print('Output scripts directory "{}" does not exist - creating it now'.format(output_scripts))
      os.makedirs(output_scripts)

   # Get a list of input files
   las_files_list = glob.glob(os.path.join(os.path.abspath(args.inlas),'*.LAS'))

   for line_num, las_file in enumerate(las_files_list):

      las_basename = os.path.split(las_file)[-1]
      las_basename = os.path.splitext(las_basename)[0]

      print('*** [{0}/{1}] {2} ***'.format(line_num+1, len(las_files_list),las_basename))

      flight_parameters = {}
      flight_parameters['basename'] = las_basename
      flight_parameters['scripts_dir'] = output_scripts
      flight_parameters['input_las'] = las_file
      flight_parameters['out_dsm'] = os.path.join(output_dir, las_basename + '_dsm.tif')
      flight_parameters['projection'] = args.projection
      flight_parameters['resolution'] = args.resolution

      out_bsub_script = os.path.join(output_scripts,'{}_process.bsub'.format(las_basename))

      write_bsub_script_for_dict(flight_parameters, out_bsub_script)

      submit_cmd = ['bsub',
                    '-q','lotus',
                    '-o',os.path.join(output_scripts,'{}_%J.o'.format(las_basename)),
                    '-e',os.path.join(output_scripts,'{}_%J.e'.format(las_basename)),
                    '-W','02:00',
                    '-n','1',
                    '<',out_bsub_script]

      if args.submit:
         print(' '.join(submit_cmd))
         # Need to use 'shell=True' for redirect
         subprocess.call(' '.join(submit_cmd),shell=True)
      else:
         print('Submit job using:')
         print(' '.join(submit_cmd))

   if args.submit:
      print('Submitted {} jobs'.format(len(las_files_list)))
      print('Check status using bjobs')
