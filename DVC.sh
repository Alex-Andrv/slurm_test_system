#!/bin/bash -i 
source activate slurm_test_system 
/nfs/home/aandreev/sat-nexus/target/release/interleave /nfs/home/aandreev/slurm_test_system/sta_comp_2023/0a4ed112f2cdc0a524976a15d1821097.cnf --backdoor-size 10  --num-iters 10000 --num-conflicts 1000 >> /nfs/home/aandreev/slurm_test_system/logs/search_product_derive_0a4ed112f2cdc0a524976a15d1821097.cnf.log