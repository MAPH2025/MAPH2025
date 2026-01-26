
# README.md

## File Structure

### MAPH
The ```MAPH``` directory contains the code for our algorithm, including:
- murmur3.h -- hash function
- maph.h -- algorithm implementation  
- main.cpp -- benchmark test implementation

### MapEmbed
The ```compare/MapEmbed``` directory contains the code for MapEmbed algorithm. The main files are:
- MapEmbed_pmem_thread.h -- MapEmbed algorithm with Pmem and multi-thread support.
- main.cpp -- benchmark test implementation.

### EEPH
The ```compare/EEPH``` directory contains the code for EEPH algorithm. The main files are:
- EEPH.h -- algorithm implementation
- main.cpp -- benchmark test implementation.

### Halo & Viper
The ```compare/Halo/``` directory contains the code for Halo and Viper algorithm. The main files are:
- /Halo/halo.hpp & halo.cpp -- algorithm implementation of Halo
- /third/viper/viper.hpp & cceh.hpp -- algorithm implementation of Viper
- hash_api.h -- defines the unified test APIs for calling different algorithms.
- benchmark.cpp -- benchmark test implementation.


## Usage
### Generate Dataset
- go to directory /YCSB
- run ```ycsb.sh``` to generate single workload according to ```workload.properties```.
- run ``` batch_ycsb.sh ``` to generate all the mixed operation workloads.
- for EEPH, further run ```/EEPH/convert_ycsb.py``` to convert the workload to proper form that EEPH require.

### Test MAPH
First configure the PMem environment, write the corresponding PMem file path into ```PMEM_PATH``` in main.cpp, then set other variables. All test functions start with test_, and you can call different test functions in the main function as needed.

```bash
g++ main.cpp -o main_pmem -lpmem -I. -std=c++17
sudo ./main_pmem
```

### Test MapEmbed
First write the corresponding PMem file path into ```pool_name_eeph``` in MapEmbed_pmem_thread.h. Then run the following commands.
```bash
g++ main.cpp -o MapEmbed -lpmem -I. -O3 -std=c++17
sudo ./MapEmbed
```

### EEPH
First write the corresponding PMem file path into ```PMEM_PATH``` in main.cpp. Then run the following commands.

```bash
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DUSE_PMEM=ON ..
make
sudo ./ycsb_benchmark --index=eeph
```

### Halo & Viper
First write the corresponding PMem file path into ```index_pool_name``` in hash_api.h. Then run the following commands to test Halo.
```bash
make HALO
sudo ./HALO
```
And run the following commands to test Viper.
```bash
make VIPER
sudo ./VIPEr
```



