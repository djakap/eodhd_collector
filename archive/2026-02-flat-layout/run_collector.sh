#!/bin/bash
# Wrapper script to run collector with proper output
cd /home/djp/eodhd_collector
source ~/miniconda3/etc/profile.d/conda.sh
conda activate tradingStrategy
python main.py "$@"
