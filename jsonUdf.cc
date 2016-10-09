// Copyright 2015 Steven Han
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
//
// Author : Steven Han <nazgul33 at gmail.com>
// Rapid json is obtainable at
//
// current status:
// - input should be a String
// - output is always a String
// - llvm udf works fine. .so udf wasn't tested
// - selector is the same with hive get_json_object, except that * operator is not supported
// - selector supports backslash escape for names with special chars (characters that should be escaped currently: '.$[]\')


// we have to define this macro before <inttypes.h> is included 
// so compile on linux works
#define __STDC_FORMAT_MACROS

#include <stdlib.h>
#include "jsonUdf.h"
#include <impala_udf/udf-debug.h>

// customize rapidjson namespace name to avoid version clashes
#define RAPIDJSON_NAMESPACE json_get_object::rapidjson
#define RAPIDJSON_NAMESPACE_BEGIN namespace json_get_object { namespace rapidjson {
#define RAPIDJSON_NAMESPACE_END   } }

#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

#include <string>
#include <vector>

#include <stdio.h>
#include <inttypes.h>

StringVal JsonGetObject(FunctionContext *context, const StringVal & jsonVal, const StringVal & selectorVal) {
    if (jsonVal.is_null) return StringVal::null();
    if (selectorVal.is_null) return StringVal::null();

    std::string json((const char*)jsonVal.ptr, jsonVal.len);
    std::string selector((const char*)selectorVal.ptr, selectorVal.len);

    RAPIDJSON_NAMESPACE::Document d;
    d.Parse(json.c_str());
    if (d.HasParseError()) {
        // context->AddWarning("Failed to parse json string");
        return StringVal::null();
    }

    const char * pSel = selector.c_str();
    RAPIDJSON_NAMESPACE::Value *currentVal = NULL;
    int i;
    bool inBracket = false;
    std::string token;

#define selectValByToken(tok) { \
    RAPIDJSON_NAMESPACE::Value& va = *currentVal;  \
    RAPIDJSON_NAMESPACE::Value key(RAPIDJSON_NAMESPACE::StringRef(tok.c_str())); \
    if (va.HasMember(key)) { \
        currentVal = &(va[key]); \
    } else { \
        /* context->AddWarning("no member"); */ \
        /* context->AddWarning(tok.c_str()); */ \
        return StringVal::null(); \
    } \
}

    if (pSel[0] != '$') {
        context->SetError("seletor should start with $");
        return StringVal::null();
    }
    currentVal = &d;
    for (i=1; i<selector.size(); i++) {
        switch (pSel[i]) {
        case '$':
            context->SetError("$ can only be placed at start");
            return StringVal::null();
        case '.':
            if (token.size() != 0)
                selectValByToken(token);
            token.clear();
            break;
        case '[':
            if (token.size() != 0) {
                selectValByToken(token);
                token.clear();
            }
            if (inBracket) {
                context->SetError("[ in [] is not allowed");
                return StringVal::null();
            }
            if (!currentVal->IsArray()) {
                //context->SetWarning("bracket on a non-array object");
                return StringVal::null();
            }
            
            inBracket = true;
            break;
        case ']':
            if (!inBracket) {
                context->SetError("] symbol without [");
                return StringVal::null();
            }
            if (token.compare("*")==0) {
                // TODO: return all in current list
                context->SetError("* symbol is not supported, yet");
                return StringVal::null();
            } else {
                RAPIDJSON_NAMESPACE::Value& va = *currentVal;
                int idx = atoi(token.c_str());
                if (idx>=va.Size()) {
                    //context->AddWarning("array index is too big");
                    return StringVal::null();
                }
                currentVal = &(va[idx]);
                token.clear();
            }
            inBracket = false;
            break;
        case '\\':
	    i++;
            if (i >= selector.size()) {
                context->SetError("encountered backslash at end of selector");
                return StringVal::null();
            }
            token += pSel[i];
            break;
        default:
            token += pSel[i];
            break;
        }
    }
    if (inBracket) {
        context->SetError("bracket is not closed");
        return StringVal::null();
    }
    if (token.size() != 0) {
        selectValByToken(token);
        token.clear();
    }

    if (currentVal->IsNull()) {
        return StringVal::null();
    }
    if (currentVal->IsBool()) {
        bool val = currentVal->GetBool();
        if (val) {
            StringVal result((uint8_t*)"true", 4);
            return result;
        } else {
            StringVal result((uint8_t*)"false", 5);
            return result;
        } 
    }
    if (currentVal->IsNumber()) {
        StringVal result(context, 32);
        int len;
        if (currentVal->IsInt64()) {
            // covers int, int64, uint
            len = snprintf((char*)result.ptr, 32, "%" PRId64, currentVal->GetInt64());
        } else if (currentVal->IsUint64()) {
            // covers uint64
            len = snprintf((char*)result.ptr, 32, "%" PRIu64, currentVal->GetUint64());
        } else {
            // covers double
            len = snprintf((char*)result.ptr, 32, "%f", currentVal->GetDouble());
        }
        result.len = len;
        return result;
    }
    if (currentVal->IsString()) {
        StringVal result(context, currentVal->GetStringLength());
        memcpy(result.ptr, currentVal->GetString(), currentVal->GetStringLength());
        return result;
    }
    // other cases : object, array
    RAPIDJSON_NAMESPACE::StringBuffer buffer;
    RAPIDJSON_NAMESPACE::Writer<RAPIDJSON_NAMESPACE::StringBuffer> writer(buffer);
    currentVal->Accept(writer);

    const char * written = buffer.GetString();
    int writtenSize = strlen(written);

    StringVal result(context, writtenSize);
    memcpy(result.ptr, written, writtenSize);
    return result;
}
