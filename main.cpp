#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <iostream>
#include <sstream>
#include <hdf5.h>

/*
 * Define operator data structure type for H5Literate callback.
 * During recursive iteration, these structures will form a
 * linked list that can be searched for duplicate groups,
 * preventing infinite recursion.
 */
struct OperatorData {
  unsigned recursion;         /* Recursion level.  0=root */
  struct OperatorData *prev;  /* Pointer to previous OperatorData */
  haddr_t group_addr;         /* Group address */
  std::string src_file_prefix;
  std::string src_file_new_prefix;
};

/*
 * Callback function to be called by H5Literate.
 */
herr_t iter_callback(hid_t loc_id, const char *name, const H5L_info_t *info, void *operator_data);
bool is_virtual(hid_t loc_id, const std::string& name);
hid_t substitute_vds_mapping(hid_t dcpl, const std::string& src_vds_path, const std::string& src_vds_path_substitute, size_t *count=NULL);
hid_t replace_vds_dset(hid_t loc_id, const std::string& name, hid_t vds_map_dcpl);
herr_t attr_iter_callback(hid_t loc_id, const char *attr_name, const H5A_info_t *ainfo, void* usr_data);
herr_t copy_attribute(hid_t src_attr_loc_id, const std::string& src_attr_name, hid_t dst_dset);
hid_t copy_attributes(hid_t src_dset, hid_t dst_dset);


/*
 * Function to check for duplicate groups in a path.
 */
int group_check (struct OperatorData *od, haddr_t target_addr);

/** program args:
 *
 * 1) file to operate on
 * 2) VDS source file path to be substituted
 * 3) New VDS source file path
 *
 */
int main (int argc, char * argv[])
{
  hid_t           file;           /* Handle */
  herr_t          status;
  H5O_info_t      infobuf;
  struct OperatorData   op_data;
  std::stringstream ss;
  ss << "Usage: h5vds-replace-paths FILE FROM TO" << std::endl << std::endl;
  ss << "  FILE: HDF5 file to operate on. The file will be modified in-place" << std::endl;
  ss << "  FROM: VDS source file path to match and replace" << std::endl;
  ss << "    TO: VDS source file path to insert in place of FROM" << std::endl;

  // print help if requested
  if (argc > 1) {
    std::string arg_one(argv[1]);
    if (arg_one == "--help" || arg_one == "-h") {
      std::cout << ss.str();
      return 0;
    }
  }

  if (argc < 4) {
    std::cerr << "ERROR not enough arguments!" << std::endl;
    std::cout << ss.str();
    return -1;
  }

  char * file_name = argv[1];

  // initialize the operator data structure.
  op_data.recursion = 0;
  op_data.prev = NULL;
  op_data.group_addr = infobuf.addr;
  op_data.src_file_prefix = std::string(argv[2]);
  op_data.src_file_new_prefix = std::string(argv[3]);

  std::cout << "Operating on file: " << file_name << std::endl;
  std::cout << "Replacing VDS source file path: " << op_data.src_file_prefix << std::endl;
  std::cout << "                          with: " << op_data.src_file_new_prefix << std::endl;

  // Open HDF5 file with read/write access
  file = H5Fopen (file_name, H5F_ACC_RDWR, H5P_DEFAULT);
  if (file < 0){
    std::cerr << "Unable to open file. Aborting." << std::endl;
    return -1;
  }
  H5Oget_info (file, &infobuf);

  /*
   * Print the root group and formatting, begin iteration.
   */
  status = H5Literate(file, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, iter_callback, (void *) &op_data);
  if (status < 0) {
    std::cerr << "H5Literate returned error..." << std::endl;
  }

  H5Fclose (file);
  return 0;
}


/************************************************************

  Callback function for H5Literate

 ************************************************************/
herr_t iter_callback(hid_t loc_id, const char *pname, const H5L_info_t *info, void *operator_data)
{
  herr_t status, return_val = 0;
  H5O_info_t info_buf;
  struct OperatorData   *op_data = (struct OperatorData *) operator_data;
  std::string name(pname);
  static std::vector<haddr_t> vds_addr_list; // list of modified VDS

  /*
   * Get type of the object we're dealing with in this callback instance
   */
  status = H5Oget_info_by_name (loc_id, name.c_str(), &info_buf, H5P_DEFAULT);
  switch (info_buf.type) {
    case H5O_TYPE_GROUP:
      std::cout << "Group: " << name << std::endl;

      /*
       * Check group address against linked list of operator
       * data structures.  To avoid infinite recursive loop.
       */
      if ( group_check (op_data, info_buf.addr) ) {
        std::cout << "Warning: loop detected..." << std::endl;
      }
      else {
        /*
         * Initialize new operator data structure and begin recursive iteration on the discovered
         * group.  The new OperatorData structure is given a pointer to the current one.
         */
        struct OperatorData next_op_data;
        next_op_data.recursion = op_data->recursion + 1;
        next_op_data.prev = op_data;
        next_op_data.group_addr = info_buf.addr;
        next_op_data.src_file_new_prefix = op_data->src_file_new_prefix;
        next_op_data.src_file_prefix = op_data->src_file_prefix;
        return_val = H5Literate_by_name(loc_id, name.c_str(), H5_INDEX_NAME,
                                        H5_ITER_NATIVE, NULL, iter_callback,
                                        (void *) &next_op_data, H5P_DEFAULT);
      }
      break;
    case H5O_TYPE_DATASET:
      std::cout << "Dataset: " << name <<  " (addr: " << info->u.address << ")" << std::endl;
      if (is_virtual(loc_id, name)) {
        // First check if this VDS has already been changed (i.e. if if is a hardlink)
        if (std::find(vds_addr_list.begin(), vds_addr_list.end(), info->u.address) != vds_addr_list.end()) {
          // This VDS has already been changed so lets hardlink it to where it is now
          // Delete the current hardlink link
          std::cout << "Hardlink detected for " << name << std::endl;
          status = H5Ldelete(loc_id, name.c_str(), H5P_LINK_ACCESS_DEFAULT);
          if (status < 0) {
            std::cerr << "failed to delete original hardlink link: " << name << std::endl;
          }
          // TODO: re-create hardlink to new dataset
        } else {
          // This has not been changed already so we will check and replace it
          hid_t vds_dset = H5Dopen(loc_id, name.c_str(), H5P_DATASET_ACCESS_DEFAULT);
          hid_t vds_dcpl = H5Dget_create_plist( vds_dset );
          hid_t new_dcpl = substitute_vds_mapping(vds_dcpl,
                                                  op_data->src_file_prefix,
                                                  op_data->src_file_new_prefix);
          H5Pclose(vds_dcpl);
          H5Dclose(vds_dset);
          if (new_dcpl > 0) {
            vds_dset = replace_vds_dset( loc_id, name, new_dcpl);
            H5Dclose(vds_dset);
            H5Pclose(new_dcpl);
            vds_addr_list.push_back(info->u.address); // register the address to the object that we have just recreated.
          }
        }
      }
      break;
    case H5O_TYPE_NAMED_DATATYPE:
      std::cout << "Datatype: " << name << std::endl;
      break;
    default:
      std::cout << "Unknown: " << name << std::endl;
  }

  return return_val;
}

/** Determine if a specific Dataset is a Virtual Dataset */
bool is_virtual(hid_t loc_id, const std::string& name)
{
  hid_t dset = H5Dopen(loc_id, name.c_str(), H5P_DATASET_ACCESS_DEFAULT);
  hid_t prop_list = H5Dget_create_plist( dset );
  H5Dclose(dset);
  hid_t layout = H5Pget_layout( prop_list );
  H5Pclose(prop_list);
  return layout == H5D_VIRTUAL;
}

/** Substitute VDS source file path prefix with a new prefix
 *
 * Generates a new Dataset Creation Property List with the same VDS mappings as the original dcpl
 * but with the paths to VDS source files substituted. If no paths are found to be substituted,
 * the function returns -1.
 *
 * @param dcpl - Dataset Creation Property List from original VDS
 * @param src_vds_path - VDS source file path (prefix) to substitute
 * @param src_vds_path_substitute - The new, substitute VDS source file path
 * @param count - output. Number of mappings in the VDS that have been substituted.
 * @return New Dataset Creation Property List
 */
hid_t substitute_vds_mapping(hid_t dcpl,
    const std::string& src_vds_path,
    const std::string& src_vds_path_substitute,
    size_t *count)
{
  hid_t new_dcpl;
  size_t c = 0;
  const size_t buffer_size = 16 * 1024;
  char vds_src_file[buffer_size];
  char vds_src_dset[buffer_size];

  size_t virtual_count;
  H5Pget_virtual_count(dcpl, &virtual_count);
  std::cout << "  VDS mapping count: " << virtual_count << std::endl;
  std::cout << "  Substituting '" << src_vds_path << "' with: '" << src_vds_path_substitute << "'" << std::endl;

  // New dset create plist with new src filenames
  new_dcpl = H5Pcreate (H5P_DATASET_CREATE);

  // Loop through all of the original VDS mappings
  for (size_t i=0; i<virtual_count; i++)
  {
    H5Pget_virtual_filename(dcpl, i, vds_src_file, buffer_size-1);
    H5Pget_virtual_dsetname(dcpl, i, vds_src_dset, buffer_size-1);
    std::cout << "    " << vds_src_file << ":" << vds_src_dset;

    hid_t vds_vspace = H5Pget_virtual_vspace(dcpl, i);
    hid_t vds_src_dspace = H5Pget_virtual_srcspace(dcpl, i);

    // Substitute the current VDS src file path with a new path/src_file_prefix
    // if the path is found. Otherwise just carry over the existing VDS src path.
    std::string src_filename(vds_src_file);
    size_t pos = src_filename.find(src_vds_path);
    if (pos != std::string::npos) { // if the string was found we replace it
      src_filename.replace(pos, src_vds_path.length(), src_vds_path_substitute);
      // double check that src and dst filename are indeed different (empty input strings could trigger false change)
      if (src_filename != vds_src_file) {
        std::cout << " --> " << src_filename << ":" << vds_src_dset << std::endl;
        c++;
      } else {
          std::cout << " (no substitution)" << std::endl;
      }
      // Add new mapping to new dataset creation property list
      H5Pset_virtual(new_dcpl, vds_vspace, src_filename.c_str(), vds_src_dset, vds_src_dspace);
    } else {
        std::cout << " (no substitution)" << std::endl;
    }
  }
  std::cout << "  Replacing: " << c << " paths." << std::endl;
  if (count != NULL) { *count = c; }

  // If the new property list doesn't have any new mappings then close it and return -1.
  if (c <= 0) {
    H5Pclose(new_dcpl);
    new_dcpl = -1;
  }
  return new_dcpl;
}

/** Replace a Virtual Dataset with a new one with a new VDS mapping
 *
 * This function gets the properties of the original dataset (from loc_id and name).
 * It deletes the dataset object and create a new one with the same datatype and space,
 * but with the new VDS mapping in vds_map_dcpl.
 *
 * @param loc_id - dataset location (i.e. group)
 * @param name - dataset name
 * @param vds_map_dcpl - New VDS mapping in dataset creation property list
 * @return New dataset id
 */
hid_t replace_vds_dset(hid_t loc_id, const std::string& name, hid_t vds_map_dcpl)
{
  hid_t new_dset = 0;
  herr_t status;

  hid_t vds_dset = H5Dopen(loc_id, name.c_str(), H5P_DATASET_ACCESS_DEFAULT);
  hid_t vds_dtype = H5Dget_type(vds_dset);
  hid_t vds_dspace = H5Dget_space(vds_dset);

  // create new DSET with temporary name, using new vds_map_dcpl
  new_dset = H5Dcreate2(loc_id, "tmp", vds_dtype, vds_dspace,
                        H5P_LINK_CREATE_DEFAULT, vds_map_dcpl, H5P_DATASET_ACCESS_DEFAULT);
  if (new_dset < 0) {
    std::cerr << "Error creating copy VDS: tmp for " << name << std::endl;
  }

  // copy all attributes from original vds to new vds
  copy_attributes(vds_dset, new_dset);

  H5Dclose(vds_dset);

  status = H5Ldelete(loc_id, name.c_str(), H5P_LINK_ACCESS_DEFAULT);
  if (status < 0) {
    std::cerr << "failed to delete original VDS link: " << name << std::endl;
  }

  // change name of new VDS to name of old VDS using H5Lmove
  status = H5Lmove(loc_id, "tmp", loc_id, name.c_str(), H5P_LINK_CREATE_DEFAULT, H5P_LINK_ACCESS_DEFAULT);
  if (status < 0) {
    std::cerr << "failed to move tmp VDS link into: " << name << std::endl;
  }

  H5Tclose(vds_dtype);
  H5Sclose(vds_dspace);
  return new_dset;
}

hid_t copy_attributes(hid_t src_dset, hid_t dst_dset)
{
  herr_t status;
  hid_t usr_data = dst_dset;
  status = H5Aiterate2(src_dset, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, attr_iter_callback, static_cast<void*>(&usr_data));
  return status;
}


herr_t attr_iter_callback(hid_t loc_id, const char *attr_name, const H5A_info_t *ainfo, void* usr_data)
{
  herr_t status;
  hid_t dst_dset = *static_cast<hid_t*>(usr_data);
  std::string attr_name_str(attr_name);
  status = copy_attribute(loc_id, attr_name_str, dst_dset);
  return status;
}

herr_t copy_attribute(hid_t src_attr_loc_id, const std::string& src_attr_name, hid_t dst_dset)
{
    herr_t status;
    std::cout << "  Copying attribute: " << src_attr_name << std::endl;
    hid_t attr_id = H5Aopen_by_name(src_attr_loc_id, ".", src_attr_name.c_str(), H5P_DEFAULT, H5P_LINK_ACCESS_DEFAULT);

    size_t data_size = H5Aget_storage_size(attr_id);
    void * pdata = calloc(1, data_size);

    hid_t dspace = H5Aget_space(attr_id);
    hid_t dtype = H5Aget_type(attr_id);
    status = H5Aread(attr_id, dtype, pdata);
    if (status < 0) {
        std::cerr << "Failed to read data (" << data_size << " bytes) from attribute: "
                  << src_attr_name << std::endl;
        H5Tclose(dtype);
        H5Sclose(dspace);
        free(pdata);
        return status;
    }

    hid_t new_attr_id = H5Acreate2(dst_dset, src_attr_name.c_str(), dtype, dspace, H5P_ATTRIBUTE_CREATE_DEFAULT, H5P_ATTRIBUTE_ACCESS_DEFAULT);
    if (new_attr_id <= 0) {
        std::cerr << "Unable to create attribute: " << src_attr_name << std::endl;
        H5Tclose(dtype);
        H5Sclose(dspace);
        free(pdata);
        return -1;
    }

    status = H5Awrite(new_attr_id, dtype, pdata);
    if (status < 0) {
        std::cerr << "Failed to write to attribute: " << src_attr_name << std::endl;
        H5Aclose(new_attr_id);
        H5Tclose(dtype);
        H5Sclose(dspace);
        free(pdata);
        return status;
    }

    H5Aclose(new_attr_id);
    H5Tclose(dtype);
    H5Sclose(dspace);
    free(pdata);
    return 0;
}

/************************************************************

  This function recursively searches the linked list of
  opdata structures for one whose address matches
  target_addr.  Returns 1 if a match is found, and 0
  otherwise.

 ************************************************************/
int group_check (struct OperatorData *od, haddr_t target_addr)
{
  if (od->group_addr == target_addr)
    return 1;       /* Addresses match */
  else if (!od->recursion)
    return 0;       /* Root group reached with no matches */
  else
    return group_check (od->prev, target_addr);
  /* Recursively examine the next node */
}