#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
"""
A script to convert ASCII LiDAR files used for ARSF data
prior to 2009 (.all) into LAS files.
Submits each file as a separate job.

For a description of the ASCII format used see:

https://arsf-dan.nerc.ac.uk/trac/wiki/Processing/LIDARDEMs

Uses convert_pre_2009_lidar.py from ARSF_Tools (https://github.com/pmlrsg/arsf_tools)

Author: Dan Clewley
Creation Date: 23/02/2016

"""
from __future__ import print_function
import argparse
import glob
import os
import subprocess

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

# Load LAStools and arsf_tools
module load contrib/arsf/lastools contrib/arsf/arsf_tools

convert_pre2009_lidar.py -i {all_file} -o {output_dir}/{basename}{out_ext}

'''.format(**flight_parameters)

   with open(output_filename,'w') as f:
      f.write(bsub_script_text)

if __name__ == '__main__':
   parser = argparse.ArgumentParser(
   description='Produce scripts for submitting data to be processed '
               'on as LOTUS jobs using bsub',)
   parser.add_argument('-i', '--inascii', type=str,
                       help='Input directory containing .all files',
                       required=True)
   parser.add_argument('-o', '--outdir', type=str,
                       default=None,
                       help='Output DSM directory',required=True)
   parser.add_argument('--outscripts', type=str,
                       help='Output directory for bsub scripts (default = same as outdir)',
                       default=None,
                       required=False)
   parser.add_argument('--submit', action='store_true',
                       help='Submit jobs for processing.',
                       required=False, default=False)
   parser.add_argument('--laz', action='store_true',
                       help='Output in LAZ format rather than LAS',
                       required=False, default=False)
   args = parser.parse_args()

   # Convert to absolute paths
   output_dir = os.path.abspath(args.outdir)
   if args.outscripts is None:
      output_scripts = output_dir
   else:
      output_scripts = os.path.abspath(args.outscripts)

   # Create output directories if they don't exist.
   if not os.path.isdir(output_dir):
      print('Output directory "{}" does not exist'
            ' - creating it now'.format(output_dir))
      os.makedirs(output_dir)

   if not os.path.isdir(output_scripts):
      print('Output scripts directory "{}" does not exist'
            ' - creating it now'.format(output_scripts))
      os.makedirs(output_scripts)

   # Get a list of input files
   all_files_list = glob.glob(os.path.join(os.path.abspath(args.inascii),'*.all'))

   for line_num, all_file in enumerate(all_files_list):

      all_basename = os.path.split(all_file)[-1]
      all_basename = os.path.splitext(all_basename)[0]

      print('*** [{0}/{1}] {2} ***'.format(line_num+1, len(all_files_list),all_basename))

      flight_parameters = {}
      flight_parameters['basename'] = all_basename
      flight_parameters['scripts_dir'] = output_scripts
      flight_parameters['input_all'] = all_file
      flight_parameters['outdir'] = output_dir
      flight_parameters['out_ext'] = '.las'
      if args.laz:
         flight_parameters['out_ext'] = '.laz'

      out_bsub_script = os.path.join(output_scripts,'{}_process.bsub'.format(all_basename))

      write_bsub_script_for_dict(flight_parameters, out_bsub_script)

      submit_cmd = ['bsub',
                    '-q','lotus',
                    '-o',os.path.join(output_scripts,'{}_%J.o'.format(all_basename)),
                    '-e',os.path.join(output_scripts,'{}_%J.e'.format(all_basename)),
                    '-W','01:00',
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
      print('Submitted {} jobs'.format(len(all_files_list)))
      print('Check status using bjobs')
