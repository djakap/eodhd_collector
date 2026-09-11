#!/bin/bash
# Run collector with unbuffered output for real-time progress

# Activate conda environment and run with unbuffered output
source ~/miniconda3/etc/profile.d/conda.sh
conda activate tradingStrategy
python -u main.py --stocks config/syariah_stocks.txt --limit 50
