// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <iostream>

#include <impala_udf/udf-test-harness.h>
#include "jsonUdf.h"

using namespace impala;
using namespace impala_udf;
using namespace std;

int main(int argc, char** argv) {
  bool passed = true;
  // Using the test harness helpers, validate the UDF returns correct results.
  // These tests validate:
  //  AddUdf(1, 2) == 3
  //  AddUdf(null, 2) == null
  {
    const char * json = "{\"project\":\"rapidjson\",\"stars\":10}";
    passed &= UdfTestHarness::ValidateUdf<StringVal, StringVal, StringVal>(
        JsonGetObject, StringVal(json), StringVal("$.project"), StringVal("rapidjson"));
  }

  {
    const char * json = "{\"store\":{\"fruit\":[{\"weight\":8,\"type\":\"apple\"},{\"weight\":9,\"type\":\"pear\"}],\"bicycle\":{\"price\":19.95,\"color\":\"red\"}},\"email\":\"amy@only_for_json_udf_test.net\",\"owner\":\"amy\",\"lang.iso 639-1\":\"en\"}";
    passed &= UdfTestHarness::ValidateUdf<StringVal, StringVal, StringVal>(
        JsonGetObject, StringVal(json), StringVal("$.owner"), StringVal("amy"));
    passed &= UdfTestHarness::ValidateUdf<StringVal, StringVal, StringVal>(
        JsonGetObject, StringVal(json), StringVal("$.store.fruit[0]"), StringVal("{\"weight\":8,\"type\":\"apple\"}"));
    passed &= UdfTestHarness::ValidateUdf<StringVal, StringVal, StringVal>(
        JsonGetObject, StringVal(json), StringVal("$.non_exist_key"), StringVal::null());
    passed &= UdfTestHarness::ValidateUdf<StringVal, StringVal, StringVal>(
        JsonGetObject, StringVal(json), StringVal("$.lang\\.iso 639-1"), StringVal("en"));
  }

  cout << "Tests " << (passed ? "Passed." : "Failed.") << endl;
  return !passed;
}
