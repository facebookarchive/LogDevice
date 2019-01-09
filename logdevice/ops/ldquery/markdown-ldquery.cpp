/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <iostream>

#include "logdevice/common/util.h"
#include "logdevice/ops/ldquery/LDQuery.h"

using facebook::logdevice::markdown_sanitize;
using std::cout;
using std::endl;
using namespace facebook::logdevice::ldquery;

/*
 * This utility outputs a Markdown-formatted document that describes
 * all available ldquery tables and their columns.
 */

static bool order_by_name(const TableMetadata& l, const TableMetadata& r) {
  return l.name < r.name;
}

int main(int argc, char** argv) {
  LDQuery ldq(""); // a client with an empty config path is only good for
                   // getting table metadata

  std::vector<TableMetadata> tables = ldq.getTables();

  std::sort(tables.begin(), tables.end(), order_by_name);

  cout << "---" << endl;
  cout << "id: LDQuery" << endl;
  cout << "title: LDQuery" << endl;
  cout << "sidebar_label: LDQuery" << endl;
  cout << "---" << endl;
  cout << endl;

  for (const TableMetadata& tm : tables) {
    cout << "## " << markdown_sanitize(tm.name) << endl;
    cout << markdown_sanitize(tm.description) << endl << endl;

    cout << "|   Column   |   Type   |   Description   |" << endl;
    cout << "|------------|:--------:|-----------------|" << endl;

    for (const TableColumn& col : tm.columns) {
      cout << "| " << markdown_sanitize(col.name) << " | "
           << col.type_as_string() << " | "
           << markdown_sanitize(col.description) << " |" << endl;
    }

    cout << endl;
  }

  return 0;
}
