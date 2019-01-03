#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
"""
A script to convert scanned aerial photographs from the NERC-ARF archive
stored in JP2 format to a Tiff using GDAL.

Author: Dan Clewley
Creation Date: 03/01/2019

"""
from __future__ import print_function
import argparse
import glob
import os
import subprocess

def get_bsub_script(input_jp2, output_dir):
    """
    Write dictionary of flight parameters to a text file.
    """
    basename = os.path.splitext(os.path.basename(input_jp2))[0]

    output_tiff = os.path.join(output_dir, basename + '.tif')

    job_parameters = {'basename' : basename,
                      'output_dir' : output_dir,
                      'input_jp2' : input_jp2,
                      'output_tiff' : output_tiff,
                      'output_dir' : output_dir}

    bsub_script_text = '''#!/bin/bash
#BSUB -J {basename}
#BSUB –o {output_dir}/%J.o
#BSUB –e {output_dir}/%J.e
#BSUB –q short-serial
#BSUB -W 01:00
#BSUB -n 1

gdal_translate -of GTiff -co "COMPRESS=LZW" {input_jp2} {output_tiff}

 '''.format(**job_parameters)

    return bsub_script_text

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
    description='Produce scripts for submitting data to be processed '
                'on as LOTUS jobs using bsub',)
    parser.add_argument('-i', '--indir', type=str,
                        help='Input directory containing .jp2 files',
                        required=True)
    parser.add_argument('-o', '--outdir', type=str,
                        default=None,
                        help='Output DSM directory',required=True)
    parser.add_argument('--submit', action='store_true',
                        help='Submit jobs for processing.',
                        required=False, default=False)
    args = parser.parse_args()

    input_dir = os.path.abspath(args.indir)
    output_dir = os.path.abspath(args.outdir)

    if not os.path.isdir(output_dir):
        print('Output directory "{}" does not exist'
              ' - creating it now'.format(output_dir))
        os.makedirs(output_dir)

    # Get a list of input files
    jp2_files_list = glob.glob(os.path.join(input_dir,'*.jp2'))

    for line_num, jp2_file in enumerate(jp2_files_list):

        print('*** [{0}/{1}] {2} ***'.format(line_num+1,
                                             len(jp2_files_list),
                                             os.path.basename(jp2_file)))

        bsub_script_text = get_bsub_script(jp2_file, output_dir)

        if args.submit:
            bsub = subprocess.Popen(["bsub"],
                                    stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            out, err = bsub.communicate(input=bsub_script_text)
        else:
            print("\n{}\n".format(bsub_script_text))

    if args.submit:
        print('Submitted {} jobs'.format(len(jp2_files_list)))
        print('Check status using bjobs')
