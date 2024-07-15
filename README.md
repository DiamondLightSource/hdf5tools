[![Build Status](https://travis-ci.org/DiamondLightSource/hdf5tools.svg?branch=master)](https://travis-ci.org/DiamondLightSource/hdf5tools)

# vdstools
Tools to manipulate HDF5 Virtual Dataset (VDS)

## Replacing paths in VDS

The small application `h5vds-replace-paths` can replace paths, either full or partial (prefix) 
paths in Virtual DataSets (VDS). 

It does this by iterating through all datasets, looking for VDS definitions, and then searching
for the specific path (string) to replace. It does **not** open the dataset, search for or access
the VDS source files. The VDS source files does not need to be available.

When a VDS with a path to replace is detected, a temporary VDS will be created, with the
substituted VDS definitions. Attributes are copied from the original VDS to the new one. Finally
the original VDS is unlinked (the link object deleted) and the new VDS renamed to take the
original name. 

Note that the original VDS can not be fully deleted from the HDF5 file as the HDF5 library does
not ship with a delete feature. However, the dataset when unlinked is no longer accessible.

Support for hardlinks (to virtual datasets): hardlinks are detected and re-created to new VDS.
 
