/******************************************************************************
 * Copyright (c) 2015-2016, TigerGraph Inc.
 * All rights reserved.
 * Project: TigerGraph Query Language
 * udf.hpp: a library of user defined functions used in queries.
 *
 * - This library should only define functions that will be used in
 *   TigerGraph Query scripts. Other logics, such as structs and helper
 *   functions that will not be directly called in the GQuery scripts,
 *   must be put into "ExprUtil.hpp" under the same directory where
 *   this file is located.
 *
 * - Supported type of return value and parameters
 *     - int
 *     - float
 *     - double
 *     - bool
 *     - string (don't use std::string)
 *     - accumulators
 *
 * - Function names are case sensitive, unique, and can't be conflict with
 *   built-in math functions and reserve keywords.
 *
 * - Please don't remove necessary codes in this file
 *
 * - A backup of this file can be retrieved at
 *     <tigergraph_root_path>/dev_<backup_time>/gdk/gsql/src/QueryUdf/ExprFunctions.hpp
 *   after upgrading the system.
 *
 ******************************************************************************/

#ifndef EXPRFUNCTIONS_HPP_
#define EXPRFUNCTIONS_HPP_

#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <algorithm>
#include <iterator>
#include <regex>
#include <gle/engine/cpplib/headers.hpp>

/**     XXX Warning!! Put self-defined struct in ExprUtil.hpp **
 *  No user defined struct, helper functions (that will not be directly called
 *  in the GQuery scripts) etc. are allowed in this file. This file only
 *  contains user-defined expression function's signature and body.
 *  Please put user defined structs, helper functions etc. in ExprUtil.hpp
 */
#include "ExprUtil.hpp"

namespace UDIMPL {
  typedef std::string string; //XXX DON'T REMOVE

  /****** BIULT-IN FUNCTIONS **************/
  /****** XXX DON'T REMOVE ****************/
  inline int str_to_int (string str) {
    return atoi(str.c_str());
  }

  inline int float_to_int (float val) {
    return (int) val;
  }

  inline string to_string (double val) {
    char result[200];
    sprintf(result, "%g", val);
    return string(result);
  }

  inline int get_minutes (string str) {
    if (str.empty()) {
      return 0;
    }
    string delim = ", ";
    int total = 0;
    std::size_t cur = str.find(delim), prev = 0;
    while (cur != string::npos) {
      string time_str = str.substr(prev, cur - prev);
      std::size_t space_ch= time_str.find(" ");
      string num_str = time_str.substr(0, space_ch), unit_str = time_str.substr(space_ch + 1);
      int num_t = str_to_int(num_str);
      if (unit_str == "days") {
        total += num_t * 60 * 24;
      } else if (unit_str == "hours") {
        total += num_t * 60;
      } else if (unit_str == "minutes") {
        total += num_t;
      }
      prev = cur + 2;
      cur = str.find(delim, prev);
    }
    return total;
  }

  inline bool is_contains (string str1, string str2) {
    return str1.find(str2) != string::npos;
  }
}
/****************************************/

#endif /* EXPRFUNCTIONS_HPP_ */
