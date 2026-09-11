#!/bin/bash
# Optimized wrapper script for faster data collection
cd /home/djp/eodhd_collector
source ~/miniconda3/etc/profile.d/conda.sh
conda activate tradingStrategy
python main_optimized.py "$@"
