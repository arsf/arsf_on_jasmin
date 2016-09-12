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

#: Default pixel size
DEFAULT_PIXEL_SIZE = 2

#: Default data type
DEFAULT_DATA_TYPE = "uint16"

#: Path to OSTN02 NTv2 Transform file
#: Downloaded from https://www.ordnancesurvey.co.uk/business-and-government/help-and-support/navigation-technology/os-net/ostn02-ntv2-format.html
OSTN02_NTV2_BIN_FILE = "/apps/contrib/arsf/arsf_common/ostn02/OSTN02_NTv2.gsb"

def get_line_parameters(level1b_file, mask_directory, nav_directory, outproj,
                        dem_file,
                        view_vectors=None,
                        data_type=DEFAULT_DATA_TYPE,
                        pixel_size=DEFAULT_PIXEL_SIZE,
                        bands="ALL",
                        rowcolmap=False,
                        atmosfile=False):
   """
   Get parameters to process a line of hyperspectral data in APL

   Requires:

   * level1b_file - Path to level1b file
   * mask_directory - Directory containing mask files
   * nav_directory - Directory containing navigation data
   * outproj - Output projection
   * dem_file - DEM file
   * view_vectors - View vectors to use (optional)
   * data_type - Output data type (optional)
   * pixel_size - Default pixel size (optional)
   * bands - List of bands to map (optionsl)
   * rowcolmap - Export map with the row and column of each pixel in the 1b data (optional)
   * atmosfile - Export file containing parameters useful for atmospheric correction (optional)

   """
   # Set up dictionary to hold parameters for line
   line_parameters = {}

   l1b_basename = os.path.split(level1b_file)[-1]
   l1b_basename = os.path.splitext(l1b_basename)[0]

   line_parameters["level1b_basename"] = l1b_basename
   line_parameters["level1b_filename"] = level1b_file
   line_parameters["mask_filename"] = os.path.join(mask_directory,
                                                   l1b_basename + "_mask.bil")
   line_parameters["navigation_filename"] = os.path.join(nav_directory,
                                      l1b_basename + "_nav_post_processed.bil")
   line_parameters["dem_file"] = dem_file

   # Other parameters
   line_parameters["output_projection"] = outproj
   line_parameters["output_projection_string"] = line_parameters["output_projection"].replace(" ","")
   # If OSNG (where transform file is passed in), set name to just "osng"
   if line_parameters["output_projection"].find("osng") > -1:
      line_parameters["output_projection_string"] = "osng"
   # If a transform file hasn't been passed in
   # use common one
   if outproj.strip().lower() == "osng":
      line_parameters["output_projection"] = "osng {}".format(OSTN02_NTV2_BIN_FILE)
      if OSTN02_NTV2_BIN_FILE is None or not os.path.isfile(OSTN02_NTV2_BIN_FILE):
         raise Exception("Could not find 'OSTN02_NTV2_BIN_FILE'. This file is "
                         "required for accurate reprojection.")

   line_parameters["outputdatatype"] = data_type
   line_parameters["pixel_size"] = str(pixel_size)
   line_parameters["bands"] = str(bands)

   if view_vectors is None:
      fov_vectors_path = level1b_dir.replace("flightlines/level1b",
                                             "sensor_FOV_vectors")
      fov_vectors_list = glob.glob(os.path.join(fov_vectors_path,"*.bil"))
      if len(fov_vectors_list) == 0:
         print("Couldn't find FOV vectors. You need to provide them using "
               "'--view_vectors'", file=sys.stderr)
         sys.exit(1)
      # If there are two (eagle and hawk) check which line we are processing
      if len(fov_vectors_list) == 2:
         fov_vectors = None
         for check_fov_vector in fov_vectors_list:
            if l1b_basename[0] == "e" and check_fov_vector.find("eagle") > -1:
               fov_vectors = check_fov_vector
            elif l1b_basename[0] == "h" and check_fov_vector.find("hawk") > -1:
               fov_vectors = check_fov_vector
         if fov_vectors is None:
            raise Exception("Couldn't find FOV vectors. You need to provide "
                            "them using '--view_vectors'")
      elif len(fov_vectors_list) > 2:
         raise Exception("Found more than one FOV vector. Need to specify using"
                         "'--view_vectors'")
      else:
         fov_vectors = fov_vectors_list[0]
   else:
      fov_vectors = args.view_vectors

   line_parameters["fov_vectors"] = fov_vectors

   # Output files
   line_parameters["masked_1b_filename"] = os.path.join(output_dir,
                                                  l1b_basename + "_masked.bil")
   line_parameters["igm_filename"] = os.path.join(output_dir,
                                                  l1b_basename + ".igm")
   line_parameters["transformed_igm_filename"] = os.path.join(output_dir,
                                                              l1b_basename + \
                 "_{}.igm".format(line_parameters["output_projection_string"]))
   line_parameters["output_filename"] = os.path.join(output_dir,
                                                     l1b_basename.replace("1b","3b") + \
                             "_{}.bil".format(line_parameters["output_projection_string"]))
   if rowcolmap:
      line_parameters["rowcol_filename"] = line_parameters["output_filename"].replace(".bil","_rowcol.bil")
   else:
      line_parameters["rowcol_filename"] = None
   if atmosfile:
      line_parameters["atmos_filename"] = os.path.join(output_dir,
                                                   l1b_basename + "_geom.bil")
   else:
      line_parameters["atmos_filename"] = None

   # Check IGM file doesn't already exist
   if os.path.isfile(line_parameters["igm_filename"]):
      err_msg = "Output IGM file '{}'".format(line_parameters["igm_filename"])
      err_msg += "already exists. Please remove and run again."
      raise Exception(err_msg)

   return line_parameters

def write_bsub_script_for_dict(line_parameters, output_filename,
                               zip_mapped=False):
   """
   Write dictionary of line parameters to a bsub script
   """
   bsub_script_text = '''#!/bin/bash
#BSUB -J {level1b_basename}
#BSUB –o {scripts_dir}/%J.o
#BSUB –e {scripts_dir}/%J.e
#BSUB –q lotus
#BSUB -W 01:00
#BSUB -n 1

# Load APL
module load contrib/arsf/apl

# Mask file
aplmask -lev1 {level1b_filename} -mask {mask_filename} -output {masked_1b_filename}

# Create IGM file
aplcorr -lev1file {masked_1b_filename} -igmfile {igm_filename} -vvfile {fov_vectors} -navfile {navigation_filename} -dem {dem_file}

# Transform projection of IGM file
apltran -inproj latlong WGS84 -igm {igm_filename} -output {transformed_igm_filename} -outproj {output_projection}

# Map file
aplmap -igm {transformed_igm_filename} -ignorediskspace -lev1 {masked_1b_filename} -mapname {output_filename} -outputdatatype {outputdatatype} -pixelsize {pixel_size} {pixel_size} -bandlist {bands}

'''.format(**line_parameters)

   if zip_mapped:
      bsub_script_text += '''
# Zip mapped file
zip -9 -j {output_filename}.zip {output_filename} {output_filename}.hdr

'''.format(**line_parameters)

   with open(output_filename,"w") as f:
      f.write(bsub_script_text)

if __name__ == "__main__":
   parser = argparse.ArgumentParser(description="Produce scripts for submitting"
                                                " data to be processed on as "
                                                "LOTUS jobs using bsub")

   parser.add_argument("inlevel1b", nargs="+",
                       type=str, help="Input level1b files")
   parser.add_argument("--outmapped", type=str,
                       default=None,
                       help="Output directory",required=True)
   parser.add_argument("--dem", type=str,
                       help="DEM used for processing",
                       required=True, default=None)
   parser.add_argument("--outproj", type=str,
                       help="Output projection (e.g., osng or "
                            "'utm_wgs84N 30'",
                       required=True, default=None)
   parser.add_argument("--outscripts", type=str,
                       help="Output directory for bsub scripts "
                            "(default = same as output directory)",
                       default=None,
                       required=False)
   parser.add_argument("--innav", type=str,
                       help="Input post-processed navigation file directory "
                            "(optional, only required if non-standard location)",
                       required=False, default=None)
   parser.add_argument("--inmasks", type=str,
                       help="Input mask files (optional, only required if not "
                            "in same directory as unmapped data)",
                       required=False, default=None)
   parser.add_argument("--view_vectors", type=str,
                       help="Sensor view vectors (optional, only required if "
                            "in non-standard location)",
                       required=False, default=None)
   parser.add_argument("--pixel_size", type=float,
                       help="Pixel size for mapped files",
                       required=False, default=DEFAULT_PIXEL_SIZE)
   parser.add_argument("--bands", type=str,
                       help="Bands to map as space separated list (default = ALL)",
                       required=False, default="ALL")
   parser.add_argument("--submit", action="store_true",
                       help="Submit jobs for processing.",
                       required=False, default=False)
   parser.add_argument("--zip", action="store_true",
                       help="Zip mapped files after processing",
                       required=False, default=False)
   args = parser.parse_args()

   if os.path.isdir(args.inlevel1b[0]):
      level1b_dir = os.path.abspath(args.inlevel1b[0])
      # Get a list of input files
      level1b_files_list = glob.glob(os.path.join(level1b_dir,"*1b.bil"))
   else:
      level1b_files_list = [os.path.abspath(f) for f in args.inlevel1b]
      level1b_dir = os.path.split(level1b_files_list[0])[0]

   output_dir = os.path.abspath(args.outmapped)
   if args.outscripts is None:
      output_scripts = output_dir
   else:
      output_scripts = os.path.abspath(args.outscripts)
   dem_file = os.path.abspath(args.dem)

   # Check if separate masks directory was provided - if not assume same as 1b files
   if args.inmasks is None:
      mask_directory = level1b_dir
   else:
      mask_directory = args.inmasks

   # Check if navigation directory was provided - if not assume same as delivery (../navigation)
   if args.innav is None:
      nav_directory = level1b_dir.replace("level1b","navigation")
   else:
      nav_directory = args.innav

   if not os.path.isdir(output_dir):
      print("Output directory '{}' does not exist - creating it now".format(output_dir))
      os.makedirs(output_dir)

   if not os.path.isdir(output_scripts):
      print("Output scripts directory '{}' does not exist - creating it now".format(output_scripts))
      os.makedirs(output_scripts)

   for line_num, level1b_file in enumerate(level1b_files_list):

      l1b_basename = os.path.split(level1b_file)[-1]
      l1b_basename = os.path.splitext(l1b_basename)[0]

      print("*** [{0}/{1}] {2} ***".format(line_num+1,
                                           len(level1b_files_list),
                                           l1b_basename))
      line_parameters = get_line_parameters(level1b_file, mask_directory,
                                            nav_directory,
                                            args.outproj,
                                            args.dem,
                                            view_vectors=args.view_vectors,
                                            pixel_size=args.pixel_size,
                                            bands=args.bands)
 
      line_parameters["scripts_dir"] = output_scripts
      out_bsub_script = os.path.join(output_scripts,
                                     "{}_process.bsub".format(l1b_basename))
      write_bsub_script_for_dict(line_parameters, out_bsub_script, args.zip)

      submit_cmd = ["bsub",
                    "-q","lotus",
                    "-o",os.path.join(output_scripts,"{}_%J.o".format(l1b_basename)),
                    "-e",os.path.join(output_scripts,"{}_%J.e".format(l1b_basename)),
                    "-W","02:00",
                    "-n","1",
                    "<",out_bsub_script]

      if args.submit:
         print(" ".join(submit_cmd))
         # Need to use "shell=True" for redirect
         subprocess.call(" ".join(submit_cmd),shell=True)
      else:
         print("Submit job using:")
         print(" ".join(submit_cmd))

   if args.submit:
      print("Submitted {} jobs".format(len(level1b_files_list)))
      print("Check status using bjobs")
