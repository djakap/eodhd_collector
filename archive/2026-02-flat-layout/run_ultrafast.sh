#!/bin/bash
# Ultra-fast wrapper script with maximum CPU optimizations
cd /home/djp/eodhd_collector
source ~/miniconda3/etc/profile.d/conda.sh
conda activate tradingStrategy
python main_ultrafast.py "$@"
