#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
"""
A script to process ARSF hyperspectral data on the JASMIN Lotus
system.

Author: Dan Clewley
Creation Date: 25/08/2015

"""
from __future__ import print_function
import argparse
import glob
import os
import subprocess
import sys

DEFAULT_PIXEL_SIZE = 2

#: Path to OSTN02 NTv2 Transform file
#: Downloaded from https://www.ordnancesurvey.co.uk/business-and-government/help-and-support/navigation-technology/os-net/ostn02-ntv2-format.html
OSTN02_NTV2_TRANSFORM_FILE = "/apps/contrib/arsf/arsf_common/ostn02/OSTN02_NTv2.gsb"

def write_bsub_script_for_dict(flight_parameters, output_filename, zip_mapped=False):
   """
   Write dictionary of flight parameters to a text file.
   """
   bsub_script_text = '''#!/bin/bash
#BSUB -J {level1b_basename}
#BSUB –o {scripts_dir}/%J.o
#BSUB –e {scripts_dir}/%J.e
#BSUB –q lotus
#BSUB -W 01:00
#BSUB -n 1

# Mask file
aplmask -lev1 {level1b_filename} -mask {mask_filename} -output {masked_1b_filename}

# Create IGM file
aplcorr -lev1file {masked_1b_filename} -igmfile {igm_filename} -vvfile {fov_vectors} -navfile {navigation_filename} -dem {dem_file}

# Transform projection of IGM file
apltran -inproj latlong WGS84 -igm {igm_filename} -output {transformed_igm_filename} -outproj {output_projection}

# Map file
aplmap -igm {transformed_igm_filename} -ignorediskspace -lev1 {masked_1b_filename} -mapname {output_filename} -outputdatatype {outputdatatype} -pixelsize {pixel_size} {pixel_size} -bandlist ALL

'''.format(**flight_parameters)

   if zip_mapped:
      bsub_script_text += '''
# Zip mapped file
zip -9 -j {output_filename}.zip {output_filename} {output_filename}.hdr

'''.format(**flight_parameters)

   with open(output_filename,'w') as f:
      f.write(bsub_script_text)

if __name__ == '__main__':
   parser = argparse.ArgumentParser(
   description='''Produce scripts for submitting data to be processed on as LOTUS jobs using bsub''',)

   parser.add_argument('--inlevel1b', type=str,
                        help='Input unmapped data directory',required=True)
   parser.add_argument('--outmapped', type=str,
                        default=None,
                        help='Output directory',required=True)
   parser.add_argument('--dem', type=str,
                        help='DEM used for processing',
                        required=True, default=None)
   parser.add_argument('--outproj', type=str,
                        help='Output projection',
                        required=True, default=None)
   parser.add_argument('--pattern', type=str,
                        help='Pattern to match when searching for input files in "--inlevel1b". By default matches all 1b files ("*1b.bil")',
                        required=False, default='*1b.bil')
   parser.add_argument('--outscripts', type=str,
                        help='Output directory for bsub scripts (default = current directory)',
                        default='.',
                        required=False)
   parser.add_argument('--innav', type=str,
                        help='Input post-processed navigation file directory (optional, only required if non-standard location)',
                        required=False, default=None)
   parser.add_argument('--inmasks', type=str,
                        help='Input mask files (optional, only required if non in same directory as unmapped data)',
                        required=False, default=None)
   parser.add_argument('--view_vectors', type=str,
                        help='Sensor view vectors (optional, only required if in non-standard location)',
                        required=False, default=None)
   parser.add_argument('--pixel_size', type=float,
                        help='Pixel size for mapped files',
                        required=False, default=DEFAULT_PIXEL_SIZE)
   parser.add_argument('--submit', action='store_true',
                        help='Submit jobs for processing.',
                        required=False, default=False)
   parser.add_argument('--zip', action='store_true',
                        help='Zip mapped files after processing',
                        required=False, default=False)
   args = parser.parse_args()

   level1b_dir = os.path.abspath(args.inlevel1b)
   output_dir = os.path.abspath(args.outmapped)
   output_scripts = os.path.abspath(args.outscripts)
   dem_file = os.path.abspath(args.dem)

   # Check if separate masks directory was provided - if not assume same as 1b files
   if args.inmasks is None:
      mask_directory = level1b_dir
   else:
      mask_directory = args.inmasks

   # Check if navigation directory was provided - if not assume same as delivery (../navigation)
   if args.innav is None:
      nav_directory = level1b_dir.replace('level1b','navigation')
   else:
      nav_directory = args.innav

   if not os.path.isdir(output_dir):
      print('Output directory "{}" does not exist - creating it now'.format(output_dir))
      os.makedirs(output_dir)

   if not os.path.isdir(output_scripts):
      print('Output scripts directory "{}" does not exist - creating it now'.format(output_scripts))
      os.makedirs(output_scripts)

   # Get a list of input files
   level1b_files_list = glob.glob(os.path.join(level1b_dir,args.pattern))

   for line_num, level1b_file in enumerate(level1b_files_list):

      l1b_basename = os.path.split(level1b_file)[-1]
      l1b_basename = os.path.splitext(l1b_basename)[0]

      print('*** [{0}/{1}] {2} ***'.format(line_num+1, len(level1b_files_list),l1b_basename))

      flight_parameters = {}
      # Input files

      flight_parameters['level1b_basename'] = l1b_basename
      flight_parameters['level1b_filename'] = level1b_file
      flight_parameters['mask_filename'] = os.path.join(mask_directory, l1b_basename + '_mask.bil')
      flight_parameters['navigation_filename'] = os.path.join(nav_directory, l1b_basename + '_nav_post_processed.bil')
      flight_parameters['dem_file'] = dem_file
      flight_parameters['scripts_dir'] = output_scripts

      # Other parameters
      flight_parameters['output_projection'] = args.outproj
      flight_parameters['output_projection_string'] = flight_parameters['output_projection'].replace(' ','')
      # If OSNG (where transform file is passed in), set name to just 'osng'
      if flight_parameters['output_projection'].find('osng') > -1:
         flight_parameters['output_projection_string'] = 'osng'
      # If a transform file hasn't been passed in
      # use common one
      if args.outproj.strip().lower() == 'osng':
         flight_parameters['output_projection'] = 'osng {}'.format(OSTN02_NTV2_TRANSFORM_FILE)
      flight_parameters['outputdatatype'] = 'uint16'
      flight_parameters['pixel_size'] = args.pixel_size

      if args.view_vectors is None:
         fov_vectors_path = level1b_dir.replace('flightlines/level1b','sensor_FOV_vectors')
         fov_vectors_list = glob.glob(os.path.join(fov_vectors_path,'*.bil'))
         if len(fov_vectors_list) == 0:
            print("Couldn't find FOV vectors. You need to provide them using '--view_vectors'", file=sys.stderr)
            sys.exit(1)
         # If there are two (eagle and hawk) check which line we are processing
         if len(fov_vectors_list) == 2:
            fov_vectors = None
            for check_fov_vector in fov_vectors_list:
               if l1b_basename[0] == 'e' and check_fov_vector.find('eagle') > -1:
                  fov_vectors = check_fov_vector
               elif l1b_basename[0] == 'h' and check_fov_vector.find('hawk') > -1:
                  fov_vectors = check_fov_vector
            if fov_vectors is None:
               print("Couldn't find FOV vectors. You need to provide them using '--view_vectors'", file=sys.stderr)
               sys.exit(1)
         else:
            fov_vectors = fov_vectors_list[0]
      else:
         fov_vectors = args.view_vectors

      flight_parameters['fov_vectors'] = fov_vectors

      # Output files
      flight_parameters['masked_1b_filename'] = os.path.join(output_dir, l1b_basename + '_masked.bil')
      flight_parameters['igm_filename'] = os.path.join(output_dir, l1b_basename + '.igm')
      flight_parameters['transformed_igm_filename'] = os.path.join(output_dir, l1b_basename + '_{}.igm'.format(flight_parameters['output_projection_string']))
      flight_parameters['output_filename'] = os.path.join(output_dir, l1b_basename.replace('1b','3b') + '_{}.bil'.format(flight_parameters['output_projection_string']))

      out_bsub_script = os.path.join(output_scripts,'{}_process.bsub'.format(l1b_basename))

      write_bsub_script_for_dict(flight_parameters, out_bsub_script, args.zip)

      submit_cmd = ['bsub',
                    '-q','lotus',
                    '-o',os.path.join(output_scripts,'{}_%J.o'.format(l1b_basename)),
                    '-e',os.path.join(output_scripts,'{}_%J.e'.format(l1b_basename)),
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
      print('Submitted {} jobs'.format(len(level1b_files_list)))
      print('Check status using bjobs')
